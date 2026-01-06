import deviantart
import requests, threading
import random
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
'''This code calls Deviant Art APIs and gathers artists gallery information'''


class DeviantArtGalleryInfo:

    def __init__(self, client_id, client_secret, TOKEN_URL, REDIRECT_URI):
        self.client_id = client_id
        self.client_secret = client_secret
        self.TOKEN_URL = TOKEN_URL
        self.REDIRECT_URI = REDIRECT_URI
        self.last_token_refresh_time = 0
        self.token_lock = threading.Lock()
        
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
            if time.time() - self.last_token_refresh_time > 20 * 60:  # Check if token has expired
                self.access_token = self.get_token()
                self.last_token_refresh_time = time.time()  # Update refresh time
                print("Token refreshed.")

    
    def get_response_rate(self, response):
        if response.status_code == 200:
            return json.loads(response.content.decode('utf-8'))
        elif response.status_code == 404:
            return 'user_done'
        elif response.status_code == 429:
            return 'too_many_requests'
        elif response.status_code == 500:
            return 'server error'
        elif response.status_code == 401:
            return 'get new token'

    def parse_gallery_data_fin2(self, devGallery):
        gallery_meta = []  # Store parsed data as a list of dictionaries
        if devGallery is not None:
            get_gallery = devGallery.get("results")
            if get_gallery is not None:
                for i in get_gallery:
                    content = i.get('content')
    
                    # Extract data and handle potential single-item lists/tuples
                    deviation_id = i['deviationid']
                    deviation_url = i['url']
                    deviation_title = i['title']
                    author_id = i['author']['userid']
                    author_name = i['author']['username']
                    author_type = i['author']['type']
                    published_on = i['published_time']
                    deviation_source = content.get('src') if content else None
                    deviation_height = content.get('height') if content else None
                    deviation_width = content.get('width') if content else None
                    deviation_transparency = content.get('transparency') if content else None
                    comments = i['stats']['comments']
                    is_mature = i['is_mature']
                    is_downloadable = i['is_downloadable']
                    favourites = i['stats']['favourites']
    
                    # Check and extract values if necessary
                    deviation_title = deviation_title[0] if isinstance(deviation_title, list) and len(deviation_title) > 0 else deviation_title
                    # Apply similar logic to other fields if they might be single-item lists/tuples
    
                    # Append data as a dictionary to the list
                    gallery_meta.append({
                        'Deviation_id': deviation_id,
                        'Deviation_url': deviation_url,
                        'Deviation_title': deviation_title,
                        'Author_id': author_id,
                        'Author_name': author_name,
                        'Author_type': author_type,
                        'Published_on': published_on,
                        'Deviation_source': deviation_source,
                        'Deviation_height': deviation_height,
                        'Deviation_width': deviation_width,
                        'Deviation_transparency': deviation_transparency,
                        'Comments': comments,
                        'is_Mature': is_mature,
                        'is_Downloadable': is_downloadable,
                        'Favourites': favourites
                    })
    
                # Create DataFrame outside the loop
                return pd.DataFrame(gallery_meta)
            else:
                print("Empty Gallery Data")
                return pd.DataFrame()  # Return an empty DataFrame
        else:
            print("No gallery found")
            return pd.DataFrame()  # Return an empty DataFrame

    def get_gallery(self,username):
        headers = {"Authorization": f"Bearer {self.access_token}"}
        params = {"username": username, "offset": 0}  # Start with offset 0
        has_more = True
        gallery_pd = pd.DataFrame()
        consecutive_empty_results = 0  # Counter for consecutive empty results
        MAX_CONSECUTIVE_EMPTY_RESULTS = 3  # Maximum allowed consecutive empty results
        if not self.access_token:
            self.access_token = self.get_token()
        if self.access_token:
            while has_more:
                try:
                        response = requests.get(
                            "https://www.deviantart.com/api/v1/oauth2/gallery/all",
                            headers=headers,
                            params=params,
                        )
                        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
                        gallery_data = response.json()
            
                        if gallery_data.get("results"):  # Check if 'results' key exists and is not empty
                            parsed_gallery = self.parse_gallery_data_fin2(gallery_data)  # Assuming parse_gallery is defined
                             # --- Exclude empty or all-NA columns before concatenation ---
                            parsed_gallery = parsed_gallery.dropna(axis=1, how='all') # Drop columns with all NA values
                            parsed_gallery = parsed_gallery[parsed_gallery.columns[parsed_gallery.notna().any()]] # Drop empty columns
                            # --- End of exclusion ---
                            
                            if len(parsed_gallery) > 0:
                                gallery_pd = pd.concat([gallery_pd, parsed_gallery], ignore_index=True) # Add ignore_index=True to avoid duplicate indices

                        else:
                            print(f"Warning: Empty 'results' for user {username}, offset {params['offset']}")
                            consecutive_empty_results += 1  # Increment counter for empty results
            
                        has_more = gallery_data["has_more"]
            
                        # Check for consecutive empty results
                        if consecutive_empty_results >= MAX_CONSECUTIVE_EMPTY_RESULTS:
                            print(f"Stopping due to {MAX_CONSECUTIVE_EMPTY_RESULTS} consecutive empty results.")
                            has_more = False  # Force stop if too many consecutive empty results
            
                        if has_more:
                            params["offset"] += len(gallery_data.get("results", []))
            
                        time.sleep(1)
                except requests.exceptions.RequestException as e:
                       print(f"Error: {e}")
                       has_more = False
            return gallery_pd

    def update_csv_with_new_data(self, new_data):
            """Updates the CSV file with new, unique data."""
            
            if os.path.exists(gall_data_path):
                existing_data = pd.read_csv(gall_data_path, on_bad_lines='skip', low_memory=False)
            else:
                existing_data = pd.DataFrame(columns=new_data.columns)  # Empty DataFrame with same columns
        
            # Filter for new data based on 'Deviation_id'
            data_to_append = new_data[~new_data['Deviation_id'].isin(existing_data['Deviation_id'])]
        
            if not data_to_append.empty:
                data_to_append.to_csv(gall_data_path, mode='a', header=not os.path.exists(gall_data_path), index=False)
                print(f"Updated CSV file with {len(data_to_append)} new rows.")
        
    def fetch_deviants_galleryInfo(self):
        """Fethc gallery data"""
        #gallery_unique_devs = "/mnt/hdd/maittewa/gallery_deviant_name_nd_deviationIds.csv"
        gall_data_path = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviants_gallData_4_5_6/uniqueDev_gall_SnwBall03_6.2.csv.gz"
        unique_deviants = pd.read_csv("/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_profileSnwball_fin1.csv.gz", low_memory=False)
        
        columns_to_append = ['Author_name']
        visited_deviants_file = "visited_deviants.pkl"
        visited_deviants = set()
        update_csv_interval = 100  # Update CSV every 100 deviants processed

        # Always read from CSV to get the most updated list of already_saved_deviants
        already_saved_deviants = pd.DataFrame(columns=columns_to_append)
        if os.path.exists(gall_data_path):
            for chunk in pd.read_csv(gall_data_path, chunksize=1000, header=0, usecols=columns_to_append, on_bad_lines='skip', low_memory=False):
                already_saved_deviants = pd.concat([already_saved_deviants, chunk[columns_to_append]], ignore_index=True)
                visited_deviants.update(already_saved_deviants["Author_name"].unique())
            print(f"Loaded unique deviants already saved on the gallery path: {len(visited_deviants)}")
    
        # Then, attempt to load additional visited deviants from the pickle file
        try:
            if os.path.exists(visited_deviants_file):
                with open(visited_deviants_file, "rb") as f:
                    visited_deviants.update(pickle.load(f))  # Update, not replace
        except EOFError:
            print("Warning: 'visited_deviants.pkl' is empty or corrupted. Ignoring it.")
            
        #save_interval = 2
        deviant_count = 0
        deviant_gall_data = pd.DataFrame()
        unique_deviants_to_check = unique_deviants['user'].tolist()
        #2.Call API for gathering the data and parsing it
        try:
            for deviant in unique_deviants_to_check:
                print(f"Total number of unique deviants are {len(unique_deviants_to_check) - len(visited_deviants)}")
                if deviant not in visited_deviants:
                    deviant_count += 1
                    #visited_deviants.add(deviant)  # Add deviant to visited set
                    print(f"Gathering gallery info {deviant}, count {deviant_count}")

                    gallery = self.get_gallery(deviant)
                    if gallery is not None:
                        deviant_gall_data = pd.concat([deviant_gall_data, gallery])
                        print(f'Gathered and parsed gallery data for {deviant}')
                        visited_deviants.add(deviant)
                        with open(visited_deviants_file, "wb") as f:
                            pickle.dump(visited_deviants, f)
                        print(f"Saved visited deviants to pickle file: {visited_deviants_file}")
                    else:
                        print(f"No gallery data of {deviant} available")
                    
                    time.sleep(random.uniform(1, 2))

                    # 3. Save Data
                    if not deviant_gall_data.empty:
                        if os.path.exists(gall_data_path):
                            # Load existing deviation ids
                            existing_deviation_ids = pd.read_csv(gall_data_path, usecols=['Deviation_id'], on_bad_lines='skip', low_memory=False)['Deviation_id'].tolist()
                        else:
                            existing_deviation_ids = []

                        # Filter out existing deviations before saving
                        deviant_gall_data = deviant_gall_data[~deviant_gall_data['Deviation_id'].isin(existing_deviation_ids)]
                        
                        # Save new deviations
                        if not deviant_gall_data.empty:
                            deviant_gall_data.to_csv(gall_data_path, mode="a", header=not os.path.exists(gall_data_path), index=False)
                            print(f"Saved gallery info for {deviant}")
                            
                        # --- Embedded else block ---
                        else:
                            print(f"Skipping saving gallery info for {deviant} (already exists)") 
                        # --- End of embedded else block ---

                else:
                    print(f"Skipping already visited {deviant}")
                    
                self.refresh_token()
        except requests.exceptions.RequestException as e:
            print(f"Exception occurred: {e}")

        print("Gallery information fetching completed.")

    


client_id = "42096"
client_secret = "97080792c6d30a4178965e41f1ca15de"
TOKEN_URL = "https://www.deviantart.com/oauth2/token"
REDIRECT_URI = "https://www.deviantart.com/oauth2/authorize"

# Initialize token refresh timer
Deviants_gall = DeviantArtGalleryInfo(client_id, client_secret, TOKEN_URL, REDIRECT_URI)
Deviants_gall.get_token()
Deviants_gall.refresh_token()
Deviants_gall.fetch_deviants_galleryInfo()
