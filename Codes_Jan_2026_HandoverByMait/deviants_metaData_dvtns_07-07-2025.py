import requests
import threading
import random
import json
import time
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
import os
import pandas as pd
from requests.exceptions import HTTPError
import pickle


class GatherMetaData:
    """
    A class to interact with the DeviantArt API for gathering metadata
    about deviations. It handles authentication, API requests, and data parsing.
    """

    def __init__(self, client_id, client_secret, TOKEN_URL, REDIRECT_URI):
        """
        Initializes the GatherMetaData instance.

        Args:
            client_id (str): The client ID for DeviantArt API authentication.
            client_secret (str): The client secret for DeviantArt API authentication.
            TOKEN_URL (str): The URL for obtaining OAuth2 tokens.
            REDIRECT_URI (str): The redirect URI for OAuth2 authentication.

        Imports Used:
            threading
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.TOKEN_URL = TOKEN_URL
        self.REDIRECT_URI = REDIRECT_URI
        self.last_token_refresh_time = 0
        self.token_lock = threading.Lock() # Used to prevent race conditions during token refresh
        self.access_token = None # Initialize access_token to None

    def get_token(self):
        """
        Obtains an access token from the DeviantArt API using client credentials flow.

        Returns:
            str: The obtained access token.

        Imports Used:
            oauthlib.oauth2.BackendApplicationClient, requests_oauthlib.OAuth2Session
        """
        client = BackendApplicationClient(client_id=self.client_id)
        scope = ['basic', 'user', 'browse']
        oauth = OAuth2Session(client=client, scope=scope, redirect_uri=self.REDIRECT_URI)
        post_data = {"grant_type": "client_credentials",
                     "redirect_uri": self.REDIRECT_URI}
        token = oauth.fetch_token(token_url=self.TOKEN_URL, client_id=self.client_id, client_secret=self.client_secret,
                                  data=post_data)
        # Extract the access token
        self.access_token = token['access_token']
        return self.access_token

    def refresh_token(self):
        """
        Refreshes the access token if it has expired (after 20 minutes).
        Uses a lock to ensure thread-safe token refreshing.

        Imports Used:
            time, threading
        """
        with self.token_lock:  # Acquire the lock
            # Check if token has expired or is not set
            if self.access_token is None or time.time() - self.last_token_refresh_time > 20 * 60:
                self.access_token = self.get_token()
                self.last_token_refresh_time = time.time()  # Update refresh time
                print("Token refreshed.")


    def get_metadata(self, devIds):
        """
        Fetches metadata for a list of deviation IDs from the DeviantArt API.

        Args:
            devIds (list): A list of deviation IDs.

        Returns:
            dict or None: The JSON response containing metadata, or None if an error occurs.

        Imports Used:
            requests, requests.exceptions.HTTPError
        """
        headers = {"Authorization": f"Bearer {self.access_token}"}
        params_meta = {"deviationids[]": devIds}
        
        # Check if we have a valid token, get one if not
        if not self.access_token:
            self.access_token = self.get_token()
            
        if self.access_token:
            try:
                api_url_devMeta = f"https://www.deviantart.com/api/v1/oauth2/deviation/metadata"
                response_devMeta = requests.get(api_url_devMeta, headers=headers, params=params_meta)
                response_devMeta.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
                return response_devMeta.json()

            except requests.exceptions.RequestException as e:
                print(f"Error getting metadata info: {e}")
                return None
        return None


    def parse_metadata(self, devMeta):
        """
        Parses the raw API response for deviation metadata into a pandas DataFrame.

        Args:
            devMeta (dict): The JSON response containing deviation metadata.

        Returns:
            pandas.DataFrame: A DataFrame containing the parsed deviation metadata,
                              or an empty DataFrame if no data or an error occurs.

        Imports Used:
            json, pandas
        """
        deviations_metadata = pd.DataFrame()
        try:
            data = devMeta  # Extract JSON data from the response
            # Check if 'metadata' key exists and is a list
            if data and 'metadata' in data and isinstance(data.get('metadata'), list):
                for i in data['metadata']:
                    # Use .get() to safely access nested keys and provide default values
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

                    # Extract and handle nested list data for tags, sponsored info etc.
                    tags_info = a.get('Tags_Info', [])
                    # Store these as JSON strings within a list for DataFrame construction
                    a['tag_name'] = [json.dumps([tag.get('tag_name') for tag in tags_info if isinstance(tag, dict)])]
                    a['Sponsered'] = [json.dumps([tag.get('sponsored') for tag in tags_info if isinstance(tag, dict)])]
                    a['Sponser'] = [json.dumps([tag.get('sponsor') for tag in tags_info if isinstance(tag, dict)])]


                    # Create a DataFrame from the single row dictionary
                    # Ensure all values are wrapped in a list for DataFrame.from_dict if not already so.
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
                        "Tags_Info": [json.dumps(a["Tags_Info"])], # Store full list of tag dicts as JSON string
                        "tag_name": a["tag_name"], # Already a list of 1 JSON string
                        "Sponsered": a["Sponsered"], # Already a list of 1 JSON string
                        "Sponser": a["Sponser"], # Already a list of 1 JSON string
                             }


                    dict_pd = pd.DataFrame(row_data)


                    deviations_metadata = pd.concat([deviations_metadata, dict_pd], ignore_index=True)
            else:
                # Log a warning or handle cases where 'metadata' key is missing or not a list
                print(f"Warning: 'metadata' key not found or not a list in API response. Response data: {data}")


        except Exception as e:
                    print(f"Error parsing metadata: {e}")
                    return pd.DataFrame()  # Return an empty DataFrame in case of error


        return deviations_metadata

    def fetch_deviations_metaData(self, target_deviant=None):
        """
        Orchestrates the fetching and saving of metadata for deviation IDs.

        It loads a list of unique deviants, iterates through their deviation IDs,
        fetches metadata for each, and saves it to a CSV file. It uses a pickle file
        to track visited deviants for resuming interrupted processes.

        Args:
            target_deviant (str, optional): If provided, only metadata for this specific deviant
                                            will be fetched and printed, bypassing the normal saving logic.
                                            Useful for debugging. Defaults to None.

        Imports Used:
            pandas, os, pickle, time, random, requests.exceptions
        """
        visited_meta_deviants = set()
        # This gallery data path is used to get Author name and Deviation ID for metadata gathering
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


        # Load gallery data to get deviation IDs for each author
        appended_gall_df = pd.DataFrame(columns=columns_to_append)
        if os.path.exists(gallery_data_path):
            try:
                # Read only the header to check for columns before reading data
                header_df = pd.read_csv(gallery_data_path, nrows=0)
                if all(col in header_df.columns for col in columns_to_append):
                    for chunk in pd.read_csv(gallery_data_path, chunksize=chunk_size, header=0, usecols=columns_to_append, on_bad_lines='skip'):
                        appended_gall_df = pd.concat([appended_gall_df, chunk[columns_to_append]], ignore_index=True)
                    print(f"Loaded gallery data: {len(appended_gall_df['Author_name'].unique())} unique authors with deviations.")
                else:
                    print(f"Warning: Required columns {columns_to_append} not found in gallery data file: {gallery_data_path}. Proceeding with available data.")

            except Exception as e:
                print(f"Error loading gallery data: {e}. Proceeding without gallery data.")


        # Load visited deviants from pickle file (if exists and is not empty) to resume progress
        try:
            if os.path.exists(visited_deviants_file) and os.path.getsize(visited_deviants_file) > 0:
                with open(visited_deviants_file, "rb") as f:
                    # Update, not replace, to ensure consistency with loaded metadata_df
                    visited_meta_deviants.update(pickle.load(f))
                print(f"Loaded {len(visited_meta_deviants)} visited deviants from {visited_deviants_file}")
            else:
                print(f"No existing pickle file found for visited deviants or it was empty. Starting with current set.")
        except (EOFError, pickle.UnpicklingError) as e:
            print(f"Warning: '{visited_deviants_file}' is empty or corrupted. Ignoring it. Error: {e}")


        # Main execution loop
        deviant_count = 0
        metaId_count = 0

        unique_deviants_to_check = set(appended_gall_df['Author_name'].tolist())
        if target_deviant: # If a target deviant is specified, only process that one
            unique_deviants_to_check = [target_deviant]

        try:
            for deviant in unique_deviants_to_check:
                # Process if not visited or if a specific target is provided (for debugging)
                if deviant not in visited_meta_deviants or target_deviant:
                    if not target_deviant: # Only add to visited set if not a target_deviant run
                        visited_meta_deviants.add(deviant)
                    deviant_count += 1
                    print(f"Gathering metadata for unique deviant: {deviant}, count: {deviant_count}")

                    # Get all unique deviation IDs for the current deviant
                    deviant_gall_data = appended_gall_df.loc[appended_gall_df['Author_name'] == deviant, 'Deviation_id']
                    devIds = deviant_gall_data.unique().tolist()
                    print(f"Total unique devIds are {len(devIds)} for {deviant}")

                    for devId in devIds:
                        metaId_count += 1
                        print(f"Gathering metadata for deviant: {deviant}, devId: {devId}, count: {metaId_count}")

                        # Check if devId has already been processed (before fetching metadata)
                        # This check is skipped if target_deviant is set for debugging purposes
                        if os.path.exists(metadata_path) and os.path.getsize(metadata_path) > 0 and not target_deviant: 
                            try:
                                 # Read only the 'Devtn_Id' column to check for existence
                                 existing_deviation_ids = pd.read_csv(metadata_path, usecols=['Devtn_Id'], on_bad_lines='skip', low_memory=False)['Devtn_Id'].tolist()
                                 if devId in existing_deviation_ids:
                                    print(f"Skipping devId: {devId} for deviant: {deviant} (already exists)")
                                    continue  # Skip to the next devId
                            except ValueError:
                                # Handle case where 'Devtn_Id' column might be missing in the existing file
                                print(f"Warning: 'Devtn_Id' column not found in existing metadata file {metadata_path}. Cannot check for existing deviation ID {devId}. Processing...")
                            except pd.errors.EmptyDataError:
                                print(f"Warning: Existing metadata file {metadata_path} is empty. Cannot check for existing deviation ID {devId}. Processing...")


                        metadata = self.get_metadata(devId)

                        if metadata is not None:
                            if target_deviant and deviant == target_deviant: # If in debug mode for a target deviant
                                print(f"Metadata for {deviant}, devId {devId}:")
                                print(json.dumps(metadata, indent=4))
                            else:
                                parsed_df = self.parse_metadata(metadata)

                                # Save metadata immediately if not empty
                                if not parsed_df.empty:
                                    # Append to CSV, add header only if file doesn't exist or is empty
                                    parsed_df.to_csv(metadata_path, mode="a", header=not os.path.exists(metadata_path) or os.path.getsize(metadata_path) == 0, index=False)
                                    print(f"Saved metadata for deviant: {deviant}, devId: {devId}")
                                    # Save the set of visited deviants to pickle file after each successful save
                                    with open(visited_deviants_file, "wb") as f:
                                        pickle.dump(visited_meta_deviants, f)
                                    print(f"Saved visited deviants to pickle file: {visited_deviants_file}")
                                else:
                                    print(f"No valid metadata parsed for devId: {devId} of {deviant}")
                        else:
                            print(f"No metadata received from API for devId: {devId} of {deviant}")

                        time.sleep(random.uniform(1, 2)) # Small delay to respect API rate limits

                else:
                    print(f"Skipping already processed deviant: {deviant}")

                self.refresh_token() # Refresh token after processing each deviant


        except requests.exceptions.RequestException as e:
            print(f"Exception occurred during data gathering: {e}")
        except Exception as e:
             print(f"An unexpected error occurred during data gathering for deviant {deviant}: {e}")

        print("Metadata fetching completed.")

# Provide API credentials
client_id = "42096"
client_secret = "97080792c6d30a4178965e41f1ca15de"
TOKEN_URL = "https://www.deviantart.com/oauth2/token"
REDIRECT_URI = "https://www.deviantart.com/oauth2/authorize"

# Instantiate the GatherMetaData class
metaDat = GatherMetaData(client_id, client_secret, TOKEN_URL, REDIRECT_URI)
# Start the data gathering process
metaDat.fetch_deviations_metaData() # Call this without argument to resume normal operation
