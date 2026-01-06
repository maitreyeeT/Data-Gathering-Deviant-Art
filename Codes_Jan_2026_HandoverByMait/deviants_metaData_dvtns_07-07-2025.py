import deviantart
import requests, threading
import random
import sqlite3
from bs4 import BeautifulSoup
import json
import time
import requests.auth
import datetime
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
import threading
import os, re
import pandas as pd
from requests.exceptions import HTTPError
import gc, pickle



class GatherMetaData:

    def __init__(self, client_id, client_secret, TOKEN_URL, REDIRECT_URI):
        self.client_id = client_id
        self.client_secret = client_secret
        self.TOKEN_URL = TOKEN_URL
        self.REDIRECT_URI = REDIRECT_URI
        self.last_token_refresh_time = 0
        self.token_lock = threading.Lock()
        self.access_token = None # Initialize access_token to None

    def get_token(self):
        client = BackendApplicationClient(client_id=client_id)
        scope = ['basic', 'user', 'browse']
        oauth = OAuth2Session(client=client, scope=scope, redirect_uri=REDIRECT_URI)
        post_data = {"grant_type": "client_credentials",
                 "redirect_uri": REDIRECT_URI}
        token = oauth.fetch_token(token_url=TOKEN_URL, client_id=client_id, client_secret=client_secret,
                                  data=post_data)
        # Extract the access token
        self.access_token = token['access_token']
        return self.access_token

    # Token refresh function
    token_lock = threading.Lock()  # Create a lock

    def refresh_token(self):
        with self.token_lock:  # Acquire the lock
            if self.access_token is None or time.time() - self.last_token_refresh_time > 20 * 60:  # Check if token has expired or is not set
                self.access_token = self.get_token()
                self.last_token_refresh_time = time.time()  # Update refresh time
                print("Token refreshed.")


    # Function to get metadata
    def get_metadata(self, devIds):
        headers = {"Authorization": f"Bearer {self.access_token}"}
        params_meta = {"deviationids[]": devIds}
        # Check if we have a valid token
        if not self.access_token:
            self.access_token = self.get_token()
        if self.access_token:
            try:

                api_url_devMeta = f"https://www.deviantart.com/api/v1/oauth2/deviation/metadata"
                response_devMeta = requests.get(api_url_devMeta, headers=headers, params=params_meta)
                response_devMeta.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
                return response_devMeta.json()

            except requests.exceptions.RequestException as e:
                print(f"Error getting info: {e}")
                return None


    def parse_metadata(self, devMeta):
        deviations_metadata = pd.DataFrame()
        try:
            data = devMeta  # Extract JSON data from the response
            # Check if 'metadata' key exists and is a list
            if data and 'metadata' in data and isinstance(data.get('metadata'), list):
                for i in data['metadata']:
                    # Use .get() to safely access nested keys and provide default values
                    # Initialize dictionary with base keys and handle potential missing lists
                    a = {"Devtn_Id": i.get('deviationid'),
                         "Devtn_Title": i.get("title"),
                         "Devtn_Descp": i.get("description"),
                         "Author_Id": i.get("author", {}).get("userid"),
                         "Author_Name": i.get("author", {}).get("username"),
                         "Author_Icon": i.get("author", {}).get("usericon"),
                         "Author_Type": i.get("author", {}).get("type"),
                         "License": i.get("license"),
                         "Allows_Comments": i.get("allows_comments"),
                         "Is_Favourited": i.get("is_favourited"),
                         "Is_Mature": i.get("is_mature"),
                         "Can_post_comments": i.get("can_post_comment"),
                         "Tags_Info": i.get("tags", []), # Default to empty list
                    }

                    # Extract and handle nested list data, ensuring they are wrapped in a list for DataFrame creation
                    tags_info = a.get('Tags_Info', [])
                    a['tag_name'] = [json.dumps([tag.get('tag_name') for tag in tags_info if isinstance(tag, dict)])] if isinstance(tags_info, list) else [json.dumps([])]
                    a['Sponsered'] = [json.dumps([tag.get('sponsored') for tag in tags_info if isinstance(tag, dict)])] if isinstance(tags_info, list) else [json.dumps([])]
                    a['Sponser'] = [json.dumps([tag.get('sponsor') for tag in tags_info if isinstance(tag, dict)])] if isinstance(tags_info, list) else [json.dumps([])]


                    # Create a DataFrame from the single row dictionary
                    # Ensure all values that were originally lists are now lists of length 1 containing the processed data
                    row_data = {
                        "Devtn_Id": [a["Devtn_Id"]],
                        "Devtn_Title": [a["Devtn_Title"]],
                        "Devtn_Descp": [a["Devtn_Descp"]],
                        "Author_Id": [a["Author_Id"]],
                        "Author_Name": [a["Author_Name"]],
                        "Author_Icon": [a["Author_Icon"]],
                        "Author_Type": [a["Author_Type"]],
                        "License": [a["License"]],
                        "Allows_Comments": [a["Allows_Comments"]],
                        "Is_Favourited": [a["Is_Favourited"]],
                        "Is_Mature": [a["Is_Mature"]],
                        "Can_post_comments": [a["Can_post_comments"]],
                        "Tags_Info": [json.dumps(a["Tags_Info"])], # Store list as JSON string
                        "tag_name": a["tag_name"], # This is already a list of length 1 containing a JSON string
                        "Sponsered": a["Sponsered"], # This is already a list of length 1 containing a JSON string
                        "Sponser": a["Sponser"], # This is already a list of length 1 containing a JSON string
                             }


                    dict_pd = pd.DataFrame(row_data)


                    deviations_metadata = pd.concat([deviations_metadata, dict_pd], ignore_index=True)
            else:
                # Log a warning or handle cases where 'metadata' key is missing or not a list
                print(f"Warning: 'metadata' key not found or not a list in API response for metadata. Response data: {data}")


        except Exception as e:
                    print(f"Error parsing metadata: {e}")
                    return pd.DataFrame()  # Return an empty DataFrame in case of error


        return deviations_metadata

    def fetch_deviations_metaData(self, target_deviant=None):
        """Executes the algorithm to fetch and save metadata for each deviation ID."""
        visited_meta_deviants = set()
        #This gallery data path is the path only for metadata gathering which includes Author name and Deviation id
        gallery_data_path = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviants_gallData_4_5_6/unqDev_gall_SnwBallForMeta-07-07-2025.csv.gz"
        metadata_path = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_metaDataSnwBall/uniqueDev_metaData_SnwBall_02.csv.gz"
        chunk_size = 10000
        columns_to_append = ['Author_name', 'Deviation_id']
        visited_deviants_file = "visited_deviants_forMetaData.pkl"  # Define the pickle file path

        # Load existing metadata deviants (if file exists and is not empty)
        if os.path.exists(metadata_path) and os.path.getsize(metadata_path) > 0:
            try:
                # Read only the header to check for columns before reading data
                header_df = pd.read_csv(metadata_path, nrows=0)
                if 'Author_Name' in header_df.columns:
                    metadata_df = pd.read_csv(metadata_path, usecols=['Author_Name'], header=0, on_bad_lines='skip')
                    visited_meta_deviants.update(metadata_df['Author_Name'].unique())
                    print(f"Loaded existing metadata deviants: {len(visited_meta_deviants)}")
                else:
                     print(f"Warning: 'Author_Name' column not found in existing metadata file: {metadata_path}. Starting fresh.")

            except Exception as e:
                print(f"Error loading existing metadata deviants: {e}. Starting fresh.")


        # Load gallery data
        appended_gall_df = pd.DataFrame(columns=columns_to_append)
        if os.path.exists(gallery_data_path):
            try:
                # Read only the header to check for columns before reading data
                header_df = pd.read_csv(gallery_data_path, nrows=0)
                if all(col in header_df.columns for col in columns_to_append):
                    for chunk in pd.read_csv(gallery_data_path, chunksize=chunk_size, header=0, usecols=columns_to_append, on_bad_lines='skip'):
                        appended_gall_df = pd.concat([appended_gall_df, chunk[columns_to_append]], ignore_index=True)
                    print(f"Loaded gallery data: {appended_gall_df.nunique()}")
                else:
                    print(f"Warning: Required columns {columns_to_append} not found in gallery data file: {gallery_data_path}. Proceeding with available data.")

            except Exception as e:
                print(f"Error loading gallery data: {e}. Proceeding without gallery data.")


        # Load visited deviants from pickle file (if exists and is not empty)
        try:
            if os.path.exists(visited_deviants_file) and os.path.getsize(visited_deviants_file) > 0:
                with open(visited_deviants_file, "rb") as f:
                    visited_meta_deviants.update(pickle.load(f))  # Update, not replace
        except (EOFError, pickle.UnpicklingError) as e:
            print(f"Warning: '{visited_deviants_file}' is empty or corrupted. Ignoring it. Error: {e}")


        # Main execution
        deviant_count = 0
        metaId_count = 0

        unique_deviants_to_check = set(appended_gall_df['Author_name'].tolist())
        if target_deviant:
            unique_deviants_to_check = [target_deviant]

        try:
            for deviant in unique_deviants_to_check:
                if deviant not in visited_meta_deviants or target_deviant: # Process if not visited or if a specific target is provided
                    if not target_deviant:
                        visited_meta_deviants.add(deviant)
                    deviant_count += 1
                    print(f"Gathering metadata for unique deviant: {deviant}, count: {deviant_count}")

                    deviant_gall_data = appended_gall_df.loc[appended_gall_df['Author_name'] == deviant, 'Deviation_id']
                    devIds = deviant_gall_data.unique().tolist()
                    print(f"Total unique devIds are {len(devIds)} for {deviant}")

                    for devId in devIds:
                        metaId_count += 1
                        print(f"Gathering metadata for deviant: {deviant}, devId: {devId}, count: {metaId_count}")

                        # Check if devId has already been processed (before fetching metadata)
                        if os.path.exists(metadata_path) and os.path.getsize(metadata_path) > 0 and not target_deviant: # Skip if not target_deviant and file is not empty
                            try:
                                 existing_deviation_ids = pd.read_csv(metadata_path, usecols=['Devtn_Id'], on_bad_lines='skip', low_memory=False)['Devtn_Id'].tolist()
                                 if devId in existing_deviation_ids:
                                    print(f"Skipping devId: {devId} for deviant: {deviant} (already exists)")
                                    continue  # Skip to the next devId
                            except ValueError:
                                # Handle case where 'DevtnId' column might be missing in the existing file
                                print(f"Warning: 'Devtn_Id' column not found in existing metadata file {metadata_path}. Cannot check for existing deviation ID {devId}. Processing...")


                        metadata = self.get_metadata(devId)

                        if metadata is not None:
                            if target_deviant and deviant == target_deviant:
                                print(f"Metadata for {deviant}, devId {devId}:")
                                print(json.dumps(metadata, indent=4))
                            else:
                                parsed_df = self.parse_metadata(metadata)

                                # Save metadata immediately if not empty
                                if not parsed_df.empty:
                                    parsed_df.to_csv(metadata_path, mode="a", header=not os.path.exists(metadata_path) or os.path.getsize(metadata_path) == 0, index=False)
                                    print(f"Saved metadata for deviant: {deviant}, devId: {devId}")
                                    #Saving the pickel file
                                    with open(visited_deviants_file, "wb") as f:
                                        pickle.dump(visited_meta_deviants, f)
                                    print(f"Saved visited deviants to pickle file: {visited_deviants_file}")
                                else:
                                    print(f"No metadata for devId: {devId} of {deviant} is available")
                        else:
                            print(f"No metadata for devId: {devId} of {deviant} is available")

                        time.sleep(random.uniform(1, 2))

                else:
                    print(f"Skipping already processed deviant: {deviant}")

                self.refresh_token()



        except requests.exceptions.RequestException as e:
            print(f"Exception occurred: {e}")


        print("Metadata fetching completed.")

# Provide API credentials
client_id = "42096"
client_secret = "97080792c6d30a4178965e41f1ca15de"
TOKEN_URL = "https://www.deviantart.com/oauth2/token"
REDIRECT_URI = "https://www.deviantart.com/oauth2/authorize"

#Call the class and the function
# Initialize token refresh timer
metaDat = GatherMetaData(client_id, client_secret, TOKEN_URL, REDIRECT_URI)
# metaDat.get_token() # This is called within refresh_token if token is None
# metaDat.refresh_token() # This is called at the end of each deviant processing
metaDat.fetch_deviations_metaData() # Call this without argument to resume normal operation
