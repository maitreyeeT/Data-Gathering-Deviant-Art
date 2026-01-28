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
        scope = ['browse']
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
    def get_metadata(self, devIds, max_retries=5, base_delay=5):
            """Fetches raw metadata from the DeviantArt API for a list of deviation IDs with retry logic for rate limits."""
            headers = {"Authorization": f"Bearer {self.access_token}"}
            # Ensure devIds is a list, even if a single ID is passed
            devIds_list = [devIds] if not isinstance(devIds, list) else devIds
            params_meta = {"deviationids[]": devIds_list}
    
            # Check if we have a valid token and refresh if needed
            if self.access_token is None or time.time() - self.last_token_refresh_time > 15 * 60: # Check token expiry before making API call
                 self.refresh_token()
    
            if not self.access_token:
                print("Could not obtain access token. Skipping API call.")
                return None
    
            api_url_devMeta = f"https://www.deviantart.com/api/v1/oauth2/deviation/metadata"
    
            for attempt in range(max_retries):
                try:
                    response_devMeta = requests.get(api_url_devMeta, headers=headers, params=params_meta)
                    response_devMeta.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
                    return response_devMeta.json()
    
                except HTTPError as e:
                    if response_devMeta.status_code == 429: # Too Many Requests
                        delay = base_delay * (2 ** attempt) + random.uniform(0, 1) # Exponential backoff with jitter
                        print(f"Rate limit hit. Retrying in {delay:.2f} seconds (Attempt {attempt + 1}/{max_retries})...")
                        time.sleep(delay)
                    else:
                        print(f"HTTP error getting info: {e}")
                        return None
                except RequestException as e:
                    print(f"Request error getting info: {e}")
                    return None
                except Exception as e:
                    print(f"An unexpected error occurred during API call: {e}")
                    return None
    
            print(f"Failed to get metadata after {max_retries} attempts due to rate limiting.")
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

   # Function to load and process gallery data in chunks
    def load_gallery_data_in_chunks(self, gallery_data_path, chunk_size, columns_to_append):
        """Loads gallery data in chunks and builds a dictionary mapping author names to deviation IDs."""
        author_deviations = {}
        if os.path.exists(gallery_data_path):
            try:
                # Read only the header to check for columns before reading data
                header_df = pd.read_csv(gallery_data_path, nrows=0)
                if all(col in header_df.columns for col in columns_to_append):
                    print(f"Loading gallery data in chunks from: {gallery_data_path}")
                    for chunk in pd.read_csv(gallery_data_path, chunksize=chunk_size, header=0, usecols=columns_to_append, on_bad_lines='skip', low_memory=False):
                        # Process each chunk to build the author_deviations dictionary
                        # Use iterrows for simplicity with smaller chunks, or apply/vectorized operations for larger chunks
                        for index, row in chunk.iterrows():
                            author_name = row['Author_name']
                            deviation_id = row['Deviation_id']
                            if pd.notna(author_name) and pd.notna(deviation_id): # Handle potential missing values
                                if author_name not in author_deviations:
                                    author_deviations[author_name] = []
                                author_deviations[author_name].append(deviation_id)

                    # Ensure unique deviation IDs for each author
                    for author, dev_ids in author_deviations.items():
                        author_deviations[author] = list(set(dev_ids))

                    print(f"Finished loading and processing gallery data. Found {len(author_deviations)} unique deviants.")
                else:
                    print(f"Warning: Required columns {columns_to_append} not found in gallery data file: {gallery_data_path}. Cannot load gallery data.")

            except Exception as e:
                print(f"Error loading gallery data: {e}. Proceeding without gallery data.")
        else:
             print(f"Gallery data file not found: {gallery_data_path}")


        return author_deviations

    # Function to fetch and parse metadata for a batch of deviation IDs
    def fetch_and_parse_metadata_batch(self, devIds_batch):
        """Fetches and parses metadata for a batch of deviation IDs."""
        batch_metadata_df = pd.DataFrame()
        if not devIds_batch:
            return batch_metadata_df

        metadata = self.get_metadata(devIds_batch)
        if metadata is not None:
            parsed_df = self.parse_metadata(metadata)
            if not parsed_df.empty:
                batch_metadata_df = parsed_df # parse_metadata already returns a DataFrame
        else:
            # print(f"No metadata available for batch of devIds: {devIds_batch}") # Avoid excessive printing for empty batches
            pass

        return batch_metadata_df

    # Function to save metadata batches to file
    def save_metadata_batch(self, batch_df, metadata_path, header_written):
        """Appends a batch of metadata to the output file."""
        if not batch_df.empty:
            # Check if the file exists and is not empty to decide whether to write header
            write_header = not os.path.exists(metadata_path) or os.path.getsize(metadata_path) == 0
            batch_df.to_csv(metadata_path, mode="a", header=write_header, index=False, compression='gzip') # Add compression
            print(f"Saved a batch of {len(batch_df)} rows of metadata to: {metadata_path}")
            return True # Indicate that header has been written
        return header_written # Return the original state if no data was saved


    # Modify the fetch_deviations_metaData method
    def fetch_deviations_metaData(self, target_deviant=None):
        """Executes the algorithm to fetch and save metadata for each deviation ID."""
        visited_meta_deviants = set()
        gallery_data_path = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviants_gallData_4_5_6/unqDev_gall_SnwBallForMeta-07-07-2025.csv.gz"
        metadata_path = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_metaDataSnwBall/uniqueDev_metaData_SnwBall_02.csv.gz"
        chunk_size = 50000  # Increased chunk size for reading gallery data
        columns_to_append = ['Author_name', 'Deviation_id']
        visited_deviants_file = "visited_deviants_forMetaData.pkl"  # Define the pickle file path
        metadata_batch_size = 50  # Define batch size for fetching/saving metadata

        # Load existing metadata deviants (if file exists and is not empty) to avoid re-processing
        if os.path.exists(metadata_path) and os.path.getsize(metadata_path) > 0:
            try:
                # Read only the header to check for columns before reading data
                header_df = pd.read_csv(metadata_path, nrows=0, compression='gzip') # Add compression
                if 'Author_Name' in header_df.columns:
                    # Read only the 'Author_Name' column to build the set of visited deviants
                    metadata_df = pd.read_csv(metadata_path, usecols=['Author_Name'], header=0, on_bad_lines='skip', compression='gzip', low_memory=False) # Add compression
                    visited_meta_deviants.update(metadata_df['Author_Name'].dropna().unique()) # Handle potential NaN values
                    print(f"Loaded existing metadata deviants from file: {len(visited_meta_deviants)}")
                    del metadata_df # Free up memory
                    gc.collect()
                else:
                     print(f"Warning: 'Author_Name' column not found in existing metadata file: {metadata_path}. Starting fresh.")

            except Exception as e:
                print(f"Error loading existing metadata deviants from file: {e}. Starting fresh.")

        # Load visited deviants from pickle file (if exists and is not empty)
        try:
            if os.path.exists(visited_deviants_file) and os.path.getsize(visited_deviants_file) > 0:
                with open(visited_deviants_file, "rb") as f:
                    visited_meta_deviants.update(pickle.load(f))  # Update, not replace
                print(f"Loaded visited deviants from pickle file: {len(visited_meta_deviants)}")
        except (EOFError, pickle.UnpicklingError) as e:
            print(f"Warning: '{visited_deviants_file}' is empty or corrupted. Ignoring it. Error: {e}")
        except Exception as e:
            print(f"Error loading visited deviants from pickle file: {e}")


        # Load and process gallery data in chunks
        author_deviations = self.load_gallery_data_in_chunks(gallery_data_path, chunk_size, columns_to_append)

        # Determine which deviants to process
        deviants_to_process = list(author_deviations.keys())
        if target_deviant:
            if target_deviant in deviants_to_process:
                deviants_to_process = [target_deviant]
            else:
                print(f"Target deviant '{target_deviant}' not found in gallery data. Exiting.")
                return

        deviant_count = 0
        # Check if header needs to be written to the output metadata file
        header_written = os.path.exists(metadata_path) and os.path.getsize(metadata_path) > 0

        try:
            for deviant in deviants_to_process:
                if deviant not in visited_meta_deviants or target_deviant: # Process if not visited or if a specific target is provided
                    if not target_deviant:
                        visited_meta_deviants.add(deviant) # Mark as visited only if not a target deviant

                    deviant_count += 1
                    print(f"Processing unique deviant: {deviant}, count: {deviant_count}")

                    devIds = author_deviations.get(deviant, [])
                    if not devIds:
                        print(f"No deviation IDs found for deviant: {deviant}. Skipping.")
                        continue

                    print(f"Total unique devIds are {len(devIds)} for {deviant}")

                    # Process deviation IDs in batches for fetching and saving metadata
                    for i in range(0, len(devIds), metadata_batch_size):
                        devIds_batch = devIds[i:i + metadata_batch_size]
                        # print(f"Processing batch of devIds for deviant: {deviant}, batch size: {len(devIds_batch)}") # Avoid excessive printing

                        batch_metadata_df = self.fetch_and_parse_metadata_batch(devIds_batch)
                        # The save_metadata_batch function now handles checking if the header needs to be written
                        self.save_metadata_batch(batch_metadata_df, metadata_path, header_written)
                        # Update header_written status after the first successful save
                        if not header_written and not batch_metadata_df.empty:
                             header_written = True


                        time.sleep(random.uniform(1, 2)) # Add a small delay between batches

                    # Save visited deviants after processing each deviant to the pickle file
                    try:
                        with open(visited_deviants_file, "wb") as f:
                            pickle.dump(visited_meta_deviants, f)
                        print(f"Saved visited deviants to pickle file: {visited_deviants_file}")
                    except Exception as e:
                        print(f"Error saving visited deviants to pickle file: {e}")


                else:
                    print(f"Skipping already processed deviant: {deviant}")

                self.refresh_token() # Refresh token after processing each deviant


        except requests.exceptions.RequestException as e:
            print(f"Exception occurred during metadata fetching: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


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