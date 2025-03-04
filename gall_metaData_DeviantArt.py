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
import gc
'''This code calls Deviant Art APIs and gathers artists profile, gallery data, watchers, friends, and metadata'''
def find_bad_lines(filepath, delimiter=','):
    """
    Finds lines in a CSV that don't have a consistent number of columns.

    Args:
        filepath: Path to the CSV file.
        delimiter: The column delimiter (default is comma).

    Returns:
        A list of line numbers (starting from 1) that have a different
        number of columns than the first row.
    """
    bad_lines = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            header = f.readline().strip()
            expected_cols = len(header.split(delimiter))
            for i, line in enumerate(f, start=2):  # Start at line 2 (line 1 is the header)
                line = line.strip()
                if not line:
                    continue  # Skip empty lines
                cols = len(line.split(delimiter))
                if cols != expected_cols:
                    bad_lines.append((i, cols))
    except UnicodeDecodeError:
        print(f"Error decoding file {filepath}. Trying with 'latin-1' encoding.")
        with open(filepath, 'r', encoding='latin-1') as f:
            header = f.readline().strip()
            expected_cols = len(header.split(delimiter))
            for i, line in enumerate(f, start=2):  # Start at line 2 (line 1 is the header)
                line = line.strip()
                if not line:
                    continue  # Skip empty lines
                cols = len(line.split(delimiter))
                if cols != expected_cols:
                    bad_lines.append((i, cols))

    return bad_lines


class DeviantArtRandomWalk:

    def __init__(self, client_id, client_secret, TOKEN_URL, REDIRECT_URI, database_path="/mnt/hdd/maittewa/random_walker_since2003_apiData.db"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.TOKEN_URL = TOKEN_URL
        self.REDIRECT_URI = REDIRECT_URI
        self.database_path = database_path
        self.create_database()
        self.conn = sqlite3.connect(self.database_path)
        self.cursor = self.conn.cursor()
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

    def random_days_since_2001(self):
        """Returns a random number of days since January 1, 2001."""
        today = datetime.date.today()
        start_date = datetime.date(2001, 1, 1)
        total_days = (today - start_date).days
        random_days = random.randint(0, total_days)
        return random_days

    def create_database(self):
        """Creates the SQLite database if it doesn't exist."""
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS deviants (
                username TEXT PRIMARY KEY,
                profile_url TEXT
            )
        """)

        conn.commit()
        conn.close()
        
    def store_data(self, deviant, profile, dev_watchers, dev_friends):
        """Stores the data in the SQLite database."""
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        cursor.execute("INSERT OR IGNORE INTO deviants (username, profile_url) VALUES (?, ?)",
                       (deviant, f"https://www.deviantart.com/{deviant}"))
        if not profile.empty:
            profile.to_sql("profile", conn, if_exists='replace', index=False)
        else:
            print("Skipping insertion of profile data because its empty")

        if not dev_watchers.empty:
            dev_watchers.to_sql("watchers", conn, if_exists='replace', index=False)
        else:
            print("Skipping insertion of watchers data because its empty")

        if not dev_friends.empty:
            dev_friends.to_sql("friends", conn, if_exists='replace', index=False)
        else:
            print("Skipping insertion of friends data because its empty")
        #metadata.to_sql("metadata", conn, if_exists='replace', index=False)

        print("Stored data for deviant:", deviant)


        
    def get_random_deviants_from_daily_deviations(self, num_deviants, date):
        """Fetches a list of random deviants from a specific tag and page."""
        # Check if we have a valid token
        if not self.access_token: 
            self.access_token = self.get_token()
        url = f"https://www.deviantart.com/api/v1/oauth2/browse/dailydeviations?access_token={self.access_token}"
        params = {
            "client_id": client_id,
            "client_secret": client_secret,
            "date": date.strftime("%Y-%m-%d"),  # Format date as YYYY-MM-DD
            # ... (other parameters if needed, e.g., limit, offset) ...
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            data = response.json()
            # results = self.da.browse_dailydeviations()
            # results = self.da.browse(tag=tag, offset=(page - 1) * 24)  # 24 results per page by default
            deviants = [deviation["author"]["username"] for deviation in data["results"]]
            random_deviants = random.sample(deviants, min(num_deviants, len(deviants)))
            return random_deviants
            # Get as many as possible
        except Exception as e:
            print(f"Error fetching deviants: {e}")
            return []


    # Function to get friends
    def get_friends_watchers_gallery(self, username, page):
        headers = {"Authorization": f"Bearer {self.access_token}"}
        params_gallery = {"username": username}
        # Check if we have a valid token
        if not self.access_token:
            self.access_token = self.get_token()
        if self.access_token:
            try:
                api_url_friends = f"https://www.deviantart.com/api/v1/oauth2/user/friends/{username}?access_token={self.access_token}"
                api_url_watchers = f"https://www.deviantart.com/api/v1/oauth2/user/watchers/{username}?access_token={self.access_token}"
                api_url_gallery = f"https://www.deviantart.com/api/v1/oauth2/gallery/folders"
                response_watchers = requests.get(api_url_watchers, params={'offset': page, 'limit': 50})
                response_friends = requests.get(api_url_friends, params={'offset': page, 'limit': 50})
                response_gallery = requests.get(api_url_gallery, headers=headers, params=params_gallery)

                return (response_watchers, response_friends, response_gallery)

            except requests.exceptions.RequestException as e:
                print(f"Error getting info: {e}")
                return None

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
    


        
    def get_profile(self,username):
        if not self.access_token:
            self.access_token = self.get_token()
        if self.access_token:
            try:
                api_url_profile = f"https://www.deviantart.com/api/v1/oauth2/user/profile/{username}?access_token={self.access_token}"
                response_profile = requests.get(api_url_profile)
                return response_profile
            except requests.exceptions.RequestException as e:
                print(f"Error getting profile info: {e}")
                return None

    def parse_user_profile(self, profile_response):
        profile_df = pd.DataFrame()
        if 'error' not in d.keys():
                a = {'user': user,
                     'real_name': d.get('real_name'),
                     'profil_url': d['profile_url'],
                     'tags': d['tagline'],
                     'country': d['countryid'],
                     'bio': d['bio'],
                     'level': d['artist_level'],
                     'specialty': d['artist_specialty']}
                a.update(d['stats'])
                dict_pd = pd.DataFrame.from_dict(a, orient='index').transpose()
                profile_df = pd.concat([profile_df, dict_pd]) 
                return profile_df
                print(f'Gathered profile information for the {user}')
        else:
            return 'error'    

    # Function to parse friends
    def parse_friends(self, friends):
        users = pd.DataFrame()
        # print(friends.keys())
        # next_offset=friends['next_offset']
        has_more = friends.get('has_more')
        for i in friends['results']:
            a = {'username': i['user']['username'],
                 'user_icon': i['user']['usericon'],
                 'type': i['user']['type'],
                 'is_watching': i['is_watching'],
                 'last_visit': i['lastvisit'],
                 'friends': i['watch']['friend'],
                 'deviations': i['watch']['deviations'],
                 'journals': i['watch']['journals'],
                 'forum_threads': i['watch']['forum_threads'],
                 'critiques': i['watch']['critiques'],
                 'scraps': i['watch']['scraps'],
                 'activity': i['watch']['activity'],
                 'collections': i['watch']['collections']}
            dict_pd = pd.DataFrame.from_dict(a, orient='index').transpose()
            users = pd.concat([users, dict_pd])
        return (has_more, users)

    def parse_watchers(self, watchers):
        users = pd.DataFrame()
        # print(friends.keys())
        # next_offset=friends['next_offset']
        has_more = watchers.get('has_more')
        for i in watchers['results']:
            a = {'username': i['user']['username'],
                 'user_icon': i['user']['usericon'],
                 'type': i['user']['type'],
                 'is_watching': i['is_watching'],
                 'last_visit': i['lastvisit'],
                 'activity': i['watch']['activity'],
                 'collections': i['watch']['collections'],
                 'critiques': i['watch']['critiques'],
                 'deviations': i['watch']['deviations'],
                 'forum_threads': i['watch']['forum_threads'],
                 'friend': i['watch']['friend'],
                 'journals': i['watch']['journals'],
                 'scraps': i['watch']['scraps']}
            dict_pd = pd.DataFrame.from_dict(a, orient='index').T
            users = pd.concat([users, dict_pd], ignore_index=True)
        return (has_more, users)

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
                            parsed_gallery = self.parse_gallery1(gallery_data)  # Assuming parse_gallery is defined
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


    def get_gallery1(self, username):
        headers = {"Authorization": f"Bearer {self.access_token}"}
        params = {"username": username, "offset": 0}
        gallery_pd = pd.DataFrame()
    
        has_more = True
        while has_more:
            try:
                response = requests.get(
                    "https://www.deviantart.com/api/v1/oauth2/gallery/all",
                    headers=headers,
                    params=params,
                )
                response.raise_for_status()
                gallery_data = response.json()
    
                #print(f"API Response for user {username}, offset {params['offset']}:")
                #print(gallery_data)
    
                if "error" in gallery_data.keys():
                    print(f"Error in API response for user {username}: {gallery_data['error_description']}")
                    has_more = False  # Stop if there's an error
                    continue
    
                if gallery_data.get("results"):
                     
                    parsed_gallery = self.parse_gallery1(gallery_data)
                    if not parsed_gallery.empty:
                        gallery_pd = pd.concat([gallery_pd, parsed_gallery])
                    else:
                        print(f"Warning: Empty 'results' for user {username}, offset {params['offset']}")
                
                has_more = gallery_data.get("has_more", False)
                if has_more:
                    params["offset"] += len(gallery_data.get("results", []))
                time.sleep(1)
    
            except requests.exceptions.RequestException as e:
                print(f"Error: {e}")
                has_more = False
        return gallery_pd

    def parse_gallery1(self, gallery_data):
        """Parses the gallery data returned by the DeviantArt API."""
        results = gallery_data.get("results", [])  # Get results or an empty list if no 'results' key
        parsed_data = []
        for item in results:
            deviation_id = item.get("deviationid")  # Use .get() to avoid KeyError
            url = item.get("url")
            title = item.get("title")
            author_id = item.get('userid')
            author_name = item.get('username')
            author_type = item.get('type')
            published_on = item.get('published_time')
            deviation_source= item.get('src') if item else None
            deviation_height= item.get('height') if item else None,
            deviation_width= item.get('width') if item else None,
            deviation_transparency = item.get('transparency') if item else None,
            deviation_comments = item.get('stats', {}).get('comments', None),
            deviation_Favourites = item.get('stats', {}).get('favourites', None)
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
        
    def watchers_friends_data(self, username):
        """Gathers watchers and watching using API."""

        watchers_pd = pd.DataFrame()
        friends_pd = pd.DataFrame()
        has_more = True
        try:
                # Get the initial batch of watchers
                for i in range(0, 5):
                    if has_more == True:
                        resp_watchers, resp_friends, resp_gallery = self.get_friends_watchers_gallery(username, i)
                        watchers = self.get_response_rate(resp_watchers)
                        friends = self.get_response_rate(resp_friends)
                        if watchers is not None:
                            has_more, parsed_watchers = self.parse_watchers(watchers)
                            if len(parsed_watchers) > 0:
                                watchers_pd = pd.concat([watchers_pd, parsed_watchers])
                        else: 
                            print(f"No watchers found for {username}")
                            return []
                    
                        if friends is not None:
                            has_more, parsed_frnds = self.parse_friends(friends)
                            if len(parsed_frnds) > 0:
                                friends_pd = pd.concat([friends_pd, parsed_frnds])
                        else: 
                            print(f"No friends found for {username}")
                            return []




        except requests.exceptions.RequestException as e:
                print(f"Error scraping DeviantArt watchers, friends and gallery because of: {e}")
                return [], []
        except Exception as e:
            print(f"Error fetching gallery, friends or watchers for {username}: {e}")
            return [], []

        return watchers_pd, friends_pd, gallery_pd

    def load_visited_deviants(self, gallery_data_path):
        visited_deviants = set()
    
        # 1. Load from pickle file first
        visited_deviants_file = "visited_deviants.pkl"
        try:
            with open(visited_deviants_file, 'rb') as f:
                visited_deviants = pickle.load(f)
        except FileNotFoundError:
            pass  # Handle case where pickle file doesn't exist yet
    
        # 2. Load from gallery_data_path (using pandas for CSV)
        try:
            if os.path.exists(gallery_data_path):
                print(f"Loading visited deviants from {gallery_data_path}...")
                # Use iterators to read only the 'Author_name' column efficiently
                for chunk in pd.read_csv(gallery_data_path, chunksize=10000, header=0, usecols=['Author_name'],
                                         on_bad_lines='skip'):
                    # Directly update the set from the column.
                    visited_deviants.update(chunk['Author_name'].unique())
                print(
                    f"Finished loading visited deviants from {gallery_data_path}. Total: {len(visited_deviants)}")
            else:
              print("No data of visited deviants")
        except pd.errors.EmptyDataError:
            print(f"Warning: {gallery_data_path} is empty.")
        except FileNotFoundError:
            print(f"Warning: {gallery_data_path} not found.")
        except Exception as e:
            print(f"An error occured trying to load the file {gallery_data_path}, {e}")
    
        return visited_deviants
        
    def fetch_gallery_deviationids_metaData(self):
        """Executes the random walk algorithm."""
        # 1. Load Existing Data and Visited Deviants:
        gallery_data_path = "/mnt/hdd/maittewa/uniqueDev_gall_RndmWalk03_4.csv"
        already_saved_deviants_file = "/mnt/hdd/maittewa/deviants_profileRandomWalkerSince2003.csv"
        metadata_path = "/mnt/hdd/maittewa/ uniqueDev_dvtnMetaRndmWalk03_1.csv"
        # Load visited deviants from file (at the beginning of the function)
        visited_deviants_file = "visited_deviants.pkl"  
        try:
            with open(visited_deviants_file, 'rb') as f:
                visited_deviants = pickle.load(f)
        except FileNotFoundError:
            visited_deviants = set()  # Start with an empty set if the file doesn't exist
        # Load visited_deviants from the gallery data file next!
        visited_deviants = self.load_visited_deviants(gallery_data_path)
        
        deviant_gall_data = pd.DataFrame()
        meta = pd.DataFrame()
        save_interval = 10
        
        if os.path.exists(already_saved_deviants_file):
            try:
                already_saved_deviants = pd.read_csv(already_saved_deviants_file)
            except pd.errors.EmptyDataError:
                print(f"Error: {already_saved_deviants_file} is empty.")
                return  # Exit the function if the file is empty
            except FileNotFoundError:
                print(f"Error: {already_saved_deviants_file} not found.")
                return # Exit the function if the file is not found
            except Exception as e:
                print(f"An error occured trying to load the file: {already_saved_deviants_file}, {e}")
                return

            unique_deviants_to_check = already_saved_deviants['user'].tolist()
            deviant_count = 0
            
            for deviant in unique_deviants_to_check:
                if deviant not in visited_deviants: # Check if the deviant has not been visited yet
                    deviant_count += 1
                    visited_deviants.add(deviant)
                    print(f"Gathering gallery info and metadata for {deviant}, count {deviant_count}")
                    gallery = self.get_gallery(deviant)
                    if gallery is not None:
                        deviant_gall_data = pd.concat([deviant_gall_data, gallery])
                        print(f'Gathered and parsed gallery data for {deviant}')
                    else:
                        print(f"No gallery data of {deviant} available")
                    time.sleep(random.uniform(1, 2))
                    if not deviant_gall_data.empty:
                        devIds = deviant_gall_data["Deviation_id"].tolist()
                        metadata = self.get_metadata(devIds)
                        if metadata:
                            parsed_df = self.parse_metadata(metadata)
                            meta = pd.concat([meta, parsed_df], ignore_index=True)
                        print(f'Gathered and parsed deviation metadata for {deviant}')

                    # 3. Save Data in Batches:
                    if deviant_count % save_interval == 0:
                        # Append to CSV with `mode="a"`
                        deviant_gall_data.to_csv(gallery_data_path, mode="a", header=not os.path.exists(gallery_data_path),
                                                index=False)
                        if not meta.empty:
                            meta.to_csv(metadata_path, mode="a", header=not os.path.exists(metadata_path), index=False)
                        # Reset DataFrames for the next batch
                        deviant_gall_data = pd.DataFrame()
                        meta = pd.DataFrame()
                        print(f"Saved data for {deviant_count} deviants.")
                    self.refresh_token()

                    # Save visited deviant
                    with open(gallery_data_path, 'a') as f:
                         f.write(deviant + '\n')

                else:
                    print(f"Skipping already visited {deviant}")
        else:
            print(f"The file {already_saved_deviants_file} does not exist.")
        # Save visited deviants to file (before exiting the function)
        with open(visited_deviants_file, 'wb') as f:
            pickle.dump(visited_deviants, f)    
        print("Random walk completed.")
    
    



client_id = "42096"
client_secret = "97080792c6d30a4178965e41f1ca15de"
TOKEN_URL = "https://www.deviantart.com/oauth2/token"
REDIRECT_URI = "https://www.deviantart.com/oauth2/authorize"

# Initialize token refresh timer
Deviants_gall_meta = DeviantArtRandomWalk(client_id, client_secret, TOKEN_URL, REDIRECT_URI)
Deviants_gall_meta.get_token()
Deviants_gall_meta.refresh_token()
Deviants_gall_meta.fetch_gallery_deviationids_metaData()