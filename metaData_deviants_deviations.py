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
                return response_devMeta

            except requests.exceptions.RequestException as e:
                print(f"Error getting info: {e}")
                return None
    

    def parse_metadata(self, devMeta):
        # Get the metadata
        deviations_metadata = pd.DataFrame()

        try:
            data = devMeta.json()  # Extract JSON data from the response
            for i in data['metadata']:
                a = {"DevtnId": i['deviationid'],
                     "DevtnTitle": i["title"],
                     "DevtnDescp": i["description"],
                     "AuthorId": i["author"]["userid"],
                     "AuthorName": i["author"]["username"],
                     "AuthorIcon": i["author"]["usericon"],
                     "AuthorType": i["author"]["type"],
                     "License": i["license"],
                     "AllowsComments": i["allows_comments"],
                     "IsFavourited": i["is_favourited"],
                     "IsMature": i["is_mature"],
                     "CanPostComments": i["can_post_comment"],
                     "TagsInfo": i["tags"]
                }
                dict_pd = pd.DataFrame.from_dict(a, orient='index').transpose()
                dict_pd['tag_name'] = dict_pd['TagsInfo'].apply(lambda tags_list: [tag['tag_name'] for tag in tags_list])
                dict_pd['Sponsered'] = dict_pd['TagsInfo'].apply(lambda tags_list: [tag['sponsored'] for tag in tags_list])
                dict_pd['Sponser'] = dict_pd['TagsInfo'].apply(lambda tags_list: [tag['sponsor'] for tag in tags_list])
                
                deviations_metadata = pd.concat([deviations_metadata, dict_pd])
        except (requests.exceptions.RequestException, KeyError, ValueError) as e:
            print(f"Error parsing metadata: {e}")
            return pd.DataFrame()  # Return an empty DataFrame in case of error
            
        return deviations_metadata

    def fetch_deviations_metaData(self):
        """Executes the random walk algorithm."""
        visited_meta_deviants = set()
        already_saved_deviants = pd.read_csv("/mnt/hdd/maittewa/deviants_profileRandomWalkerSince2003.csv")
        gallery_data_path = "/mnt/hdd/maittewa/uniqueDev_gall_RndmWalk03_4.csv"
        metadata_path = "/mnt/hdd/maittewa/uniqueDev_dvtnMetaRndmWalk03_1.csv"
        chunk_size = 10000
        columns_to_append = ['Author_name', 'Deviation_id'] 
    
        # Load existing metadata deviants (if file exists)
        if os.path.exists(metadata_path):
            metadata_df = pd.read_csv(metadata_path, usecols=['AuthorName'], header=0, on_bad_lines='skip')
            visited_meta_deviants.update(metadata_df['AuthorName'].unique())
            print(f"Loaded existing metadata deviants: {len(visited_meta_deviants)}")
    
        # Load gallery data (if file exists)
        appended_gall_df = pd.DataFrame(columns=columns_to_append)
        if os.path.exists(gallery_data_path):
            for chunk in pd.read_csv(gallery_data_path, chunksize=chunk_size, header=0, usecols=columns_to_append, on_bad_lines='skip'):
                appended_gall_df = pd.concat([appended_gall_df, chunk[columns_to_append]], ignore_index=True)
            print(f"Loaded gallery data: {len(appended_gall_df)} rows")
    
        # Main execution
        deviant_count = 0
        batch_size = 10  
        try:
            unique_deviants_in_gallery = appended_gall_df['Author_name'].unique()
            for deviant in unique_deviants_in_gallery:
                if deviant not in visited_meta_deviants:
                    visited_meta_deviants.add(deviant)
                    deviant_count += 1
                    print(f"Gathering metadata for unique deviant: {deviant}, count: {deviant_count}")
    
                    deviant_gall_data = appended_gall_df.loc[appended_gall_df['Author_name'] == deviant, 'Deviation_id']
                    devIds = deviant_gall_data.tolist()
                    metadata_chunks = [devIds[i:i + chunk_size] for i in range(0, len(devIds), chunk_size)]
                    
                    meta = pd.DataFrame()  # Reset meta for each deviant
                    for chunk in metadata_chunks:
                        metadata = self.get_metadata(chunk)
                        if metadata:
                            parsed_df = self.parse_metadata(metadata)
                            meta = pd.concat([meta, parsed_df], ignore_index=True) 
    
                    # Save metadata only if it was fetched
                    if not meta.empty:
                        meta.to_csv(metadata_path, mode="a", header=not os.path.exists(metadata_path), index=False)
                        print(f"Saved metadata for {deviant}")
    
                        # Refresh token occasionally (e.g., every 10 deviants)
                        if deviant_count % 10 == 0:
                            self.refresh_token()
                else:
                    print(f"Skipping already processed deviant: {deviant}")
    
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
metaDat.get_token()
metaDat.refresh_token()
metaDat.fetch_deviations_metaData()