import deviantart
import requests, threading
import random
import sqlite3
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


    def parse_gallery_data(self, devGallery):
        gallery_meta = pd.DataFrame()
        if devGallery is not None:
            get_gallery = devGallery.get("results")
            if get_gallery is not None:
                for i in get_gallery: # Handle potential missing 'metadata' key
                    content = i.get('content') # Get 'content' value, or None if missing
                    a = {'Deviation_id': i['deviationid'],
                         'Deviation_url': i['url'],
                         'Deviation_title': i['title'],
                         'Author_id': i['author']['userid'],
                         'Author_name': i['author']['username'],
                         'Author_type': i['author']['type'],
                         'Published_on': i['published_time'],
                         'Deviation_source': content.get('src') if content else None,
                         'Deviation_height': content.get('height') if content else None,
                         'Deviation_width': content.get('width') if content else None,
                         'Deviation_transparency': content.get('transparency') if content else None,
                         'Comments': i['stats']['comments'],
                         'is_Mature': i['is_mature'],
                         'is_Downloadable': i['is_downloadable'],
                         'Favourites': i['stats']['favourites']}
                    dict_pd = pd.DataFrame([a]).T  # Create DataFrame from a list of dictionaries
                    dict_pd.columns = dict_pd.iloc[0]  # Set columns to the keys of the dictionary
                    dict_pd = dict_pd[1:]  # Remove the first row (which contained the keys)
                    #    dict_pd['tag_name'] = dict_pd['TagsInfo'].apply(lambda tags_list: [tag['tag_name'] for tag in tags_list])
                    #    dict_pd['Sponsered'] = dict_pd['TagsInfo'].apply(lambda tags_list: [tag['sponsored'] for tag in tags_list])
                    #    dict_pd['Sponser'] = dict_pd['TagsInfo'].apply(lambda tags_list: [tag['sponsor'] for tag in tags_list])
                        
                    gallery_meta = pd.concat([gallery_meta, dict_pd], ignore_index=True)
                return (gallery_meta)
            else:
                print("Empty Gallery Data")
        else:
            print("No gallery found")
            return []

    def parse_gallery_data_fin(self, gallery_data):
        """Parses the gallery data returned by the DeviantArt API."""
        results = gallery_data.get("results", [])  # Get results or an empty list if no 'results' key
        parsed_data = []
        if results is not None:
            for item in results:
                deviation_id = item.get("deviationid"),  # Use .get() to avoid KeyError
                url = item.get("url"),
                title = item.get("title"),
                author_id = item.get("author",{}).get('userid'),
                author_name = item.get("author",{}).get('username'),
                author_type = item.get("author",{}).get('type'),
                published_on = item.get('published_time'),
                deviation_source= item.get('src'),
                deviation_height= item.get('height'),
                deviation_width= item.get('width'),
                deviation_transparency = item.get('transparency'),
                deviation_comments = item.get('stats', {}),
                deviation_Favourites = item.get('stats', {})
                # Add more fields as needed
                if deviation_id is not None:
                    parsed_data.append({
                        "Deviation_id": deviation_id,
                        "URL": url,
                        "Title": title,
                        "Author_id": author_id,
                        "Author_name": author_name,
                        "Author_type": author_type,
                        "Published_on": published_on,
                        "Deviation_source": deviation_source,
                        "Deviation_height": deviation_height,
                        "Deviation_width": deviation_width,
                        "Deviation_transparency": deviation_transparency,
                        "Deviation_comments": deviation_comments,
                        "Deviation_Favourites": deviation_Favourites
                        # Add more fields here
                    })
                else:
                    print("Item skipped because no deviation_id found")
        
            return pd.DataFrame(parsed_data)

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
                            parsed_gallery = self.parse_gallery_data_fin(gallery_data)  # Assuming parse_gallery is defined
                            if len(parsed_gallery) > 0:
                                gallery_pd = pd.concat([gallery_pd, parsed_gallery])
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

        
    def fetch_deviants_galleryInfo(self):
        """Executes the random walk algorithm."""
        visited_deviants = set() 
        #gallery_unique_devs = "/mnt/hdd/maittewa/gallery_deviant_name_nd_deviationIds.csv"
        gall_data_path = "/mnt/hdd/maittewa/uniqueDev_gall_RndmWalk03_6.csv"
        unique_deviants = pd.read_csv("/mnt/hdd/maittewa/uniqueDevs_gall_prof_RndmWalkr03.csv")
        #columns_to_append = ['Author_name', 'Deviation_id']
        #appended_gall_df = pd.DataFrame(columns=columns_to_append)
        #if os.path.exists(gallery_unique_devs):
         #   for chunk in pd.read_csv(gallery_unique_devs, chunksize=10000, header=0, usecols=columns_to_append, on_bad_lines='skip'):
         #       appended_gall_df = pd.concat([appended_gall_df, chunk[columns_to_append]], ignore_index=True)
         #       visited_deviants.update(appended_gall_df["Author_name"].unique())
        #print(f"Loaded unique deviants in gallery: {len(visited_deviants)}")
        
        save_interval = 2
        unique_deviants_to_check = unique_deviants['UniqueValues'].tolist()
        deviant_count = 0
        deviant_gall_data = pd.DataFrame()
        
        #2.Call API for gathering the data and parsing it
        try:
            for deviant in unique_deviants_to_check:
                #print(f"Total number of unique deviants are {len(visited_deviants)}")
                if deviant not in visited_deviants:
                    deviant_count += 1
                    visited_deviants.add(deviant)
                    print(f"Gathering gallery info {deviant}, count {deviant_count}")

                    gallery = self.get_gallery(deviant)
                    if gallery is not None:
                        deviant_gall_data = pd.concat([deviant_gall_data, gallery])
                        print(f'Gathered and parsed gallery data for {deviant}')
                    else:
                        print(f"No gallery data of {deviant} available")
                    
                    time.sleep(random.uniform(1, 2))

                    # 3. Save Data
                    if not deviant_gall_data.empty:
                        deviant_gall_data.to_csv(gall_data_path, mode="a", header=not os.path.exists(gall_data_path), index=False)
                        #deviant_gall_data[columns_to_append].to_csv(gallery_unique_devs, mode = "a", header=False, index=False)
                        print(f"Saved gallery info for {deviant}")  
                        #gall_df = pd.read_csv(gall_data_path)
                        #print(f"Shape of the dataframe is {gall_df.info()}")

                    self.refresh_token()


                else:
                    print(f"Skipping already visited {deviant}")
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