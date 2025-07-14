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


class DeviantArtWatchersFriends:

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
            if response is None: # Handle case where response itself is None
                return None
            if response.status_code == 200:
                try:
                    return json.loads(response.content.decode('utf-8'))
                except json.JSONDecodeError:
                    print(f"Error decoding JSON from response: {response.content}")
                    return 'json_decode_error'
            elif response.status_code == 404:
                return 'user_done'
            elif response.status_code == 429:
                return 'too_many_requests'
            elif response.status_code == 500:
                return 'server error'
            elif response.status_code == 401:
                return 'get new token'
            else:
                return f'unexpected_status_code_{response.status_code}'
    def get_friends_watchers(self, username, page):
        headers = {"Authorization": f"Bearer {self.access_token}"}
        params_gallery = {"username": username}
        max_retries = 5
        # Check if we have a valid token
        if not self.access_token:
            self.access_token = self.get_token()
        if self.access_token:
            api_url_friends = f"https://www.deviantart.com/api/v1/oauth2/user/friends/{username}?access_token={self.access_token}"
            api_url_watchers = f"https://www.deviantart.com/api/v1/oauth2/user/watchers/{username}?access_token={self.access_token}"
                
            for retry_count in range(max_retries):
                try:
                    response_watchers = requests.get(api_url_watchers, params={'offset': page, 'limit': 50})
                    response_friends = requests.get(api_url_friends, params={'offset': page, 'limit': 50})
# Check for rate limit error specifically
                    if response_watchers.status_code == 429 or response_friends.status_code == 429:
                        wait_time = 20 ** retry_count + random.uniform(5, 10) # Exponential backoff with jitter
                        print(f"Rate limit hit. Retrying in {wait_time:.2f} seconds (Attempt {retry_count + 1}/{max_retries}).")
                        time.sleep(wait_time)
                        self.refresh_token() # Consider refreshing token on 429 as well
                        continue # Retry the request

                    # Check for other potential non-200 status codes before raising for status
                    if response_watchers.status_code != 200 or response_friends.status_code != 200:
                         print(f"Non-200 status code received for {username} (offset {offset}, limit {limit}). Watchers Status: {response_watchers.status_code}, Friends Status: {response_friends.status_code}")
                         # Return the responses even if not 200, so get_response_rate can handle them
                         return (response_watchers, response_friends)


                    response_watchers.raise_for_status() # Raise HTTPError for other bad responses (if not handled above)
                    response_friends.raise_for_status()

                    return (response_watchers, response_friends)

                except requests.exceptions.RequestException as e:
                    print(f"Error getting info for {username} (offset {offset}, limit {limit}): {e}")
                    if retry_count < max_retries - 1:
                        wait_time = 20 ** retry_count + random.uniform(0, 1)
                        print(f"Request failed. Retrying in {wait_time:.2f} seconds (Attempt {retry_count + 1}/{max_retries}).")
                        time.sleep(wait_time)
                        self.refresh_token() # Refresh token before retrying
                    else:
                        print(f"Max retries ({max_retries}) reached for {username} (offset {offset}, limit {limit}). Giving up.")
                        return None, None # Return None after max retries

            return None, None # Return None if loop finishes without success


    # Function to parse friends
    def parse_friends(self, friends):
        users = pd.DataFrame()
        # print(friends.keys())
        # next_offset=friends['next_offset']
        has_more = friends.get('has_more')
        for i in friends['results']:
            a = {'Friends name': i['user']['username'],
                 'user_icon': i['user']['usericon'],
                 'type': i['user']['type'],
                 'is_watching': i['is_watching'],
                 'watches_you': i['watches_you'],
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
            a = {'Watchers name': i['user']['username'],
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

    

        
    def watchers_friends_data(self, username):
        """Gathers watchers and watching using API."""

        watchers_pd = pd.DataFrame()
        friends_pd = pd.DataFrame()
        has_more = True
        try:
                # Get the initial batch of watchers and friends
                for i in range(0, 10):
                    if has_more == True:
                        resp_watchers, resp_friends = self.get_friends_watchers(username, i)
                        watchers = self.get_response_rate(resp_watchers)
                        friends = self.get_response_rate(resp_friends)
                        if watchers is not None:
                            has_more, parsed_watchers = self.parse_watchers(watchers)
                            if len(parsed_watchers) > 0:
                                parsed_watchers["Deviant"] = username
                                watchers_pd = pd.concat([watchers_pd, parsed_watchers])
                        else: 
                            print(f"No watchers found for {username}")
                            return []
                    
                        if friends is not None:
                            has_more, parsed_frnds = self.parse_friends(friends)
                            if len(parsed_frnds) > 0:
                                parsed_frnds["Deviant"] = username
                                friends_pd = pd.concat([friends_pd, parsed_frnds])
                        else: 
                            print(f"No friends found for {username}")
                            return []




        except requests.exceptions.RequestException as e:
                print(f"Error scraping DeviantArt watchers, friends because of: {e}")
                return [], []
        except Exception as e:
            print(f"Error fetching friends or watchers for {username}: {e}")
            return [], []

        return watchers_pd, friends_pd    
        
    def gather_watchers_friends(self):
        #current_tag = start_tag
        visited_deviants = set()
        deviant_count = 0  # Keep track of processed deviants
        # Establish a single database connection outside the loop
        #count_for_reauth = 0
        deviant_batch = []
        user_name = []
        #watchers = pd.read_csv("/mnt/hdd/maittewa/deviants_watchersRndmWalk03_2.csv")
        #friends = pd.read_csv("/mnt/hdd/maittewa/deviants_friendsRndmWalk03_2.csv")
        unique_deviants = pd.read_csv("/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_metaDataSnwBall/uniqueDev_metaData_SnwBall_02.csv.gz") #pd.DataFrame()
        #unique_deviants_to_gather = self.append_unique_usernames(watchers,friends,unique_deviants)
        unique_dev_list = set(unique_deviants["Author_Name"].tolist()) #unique_deviants_to_gather["unique_dev"].tolist()
        dev_watcrs = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_watchersSnwball_4/metaDev_watchers-11-07-2025.csv.gz"
        dev_frnds = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_friendsSnwBall_1-4/metaDev_friends-11-07-2025.csv.gz"
        # Define the path for the pickle file
        visited_deviants_pickle_path = "visited_deviants_watchers_friends.pkl"

        # Load visited deviants from the pickle file if it exists
        if os.path.exists(visited_deviants_pickle_path):
            try:
                with open(visited_deviants_pickle_path, 'rb') as f:
                    visited_deviants = pickle.load(f)
                print(f"Loaded {len(visited_deviants)} visited deviants from {visited_deviants_pickle_path}")
            except (EOFError, pickle.UnpicklingError) as e:
                print(f"Error loading visited deviants from {visited_deviants_pickle_path}: {e}. Starting with an empty visited list.")
                visited_deviants = set() # Start fresh if file is empty or corrupted


        try:
            for deviant in unique_dev_list:
                # Initialize deviant_watchers and deviant_friends to None at the start of each loop iteration
                deviant_watchers = None
                deviant_friends = None

                if deviant not in visited_deviants:
                    deviant_count += 1
                    visited_deviants.add(deviant)
                    print(f"Fetching data for unique deviant: {deviant}")
                    watchers_friends = self.watchers_friends_data(deviant) # Call the corrected watchers_friends_data
                    if watchers_friends is not None and len(watchers_friends) == 2:
                        deviant_watchers, deviant_friends = watchers_friends
                    else:
                        print(f"No watchers, friends of {deviant} available or an error occurred.")

                    # Check if deviant_watchers is a DataFrame before saving
                    if isinstance(deviant_watchers, pd.DataFrame) and not deviant_watchers.empty:
                            print(f"fetched deviant watchers for {deviant}")
                            deviant_watchers.to_csv(dev_watcrs, mode="a", header=not os.path.exists(dev_watcrs), index=False)
                            print(f"Saved {deviant} watchers")
                            time.sleep(5) # Add delay after saving watchers
                    else:
                            print(f"Empty or invalid deviant watchers data for {deviant}")
                            # Decide if you want to continue or skip to the next deviant
                            # For now, we will continue to process friends if available
                            pass


                    # Check if deviant_friends is a DataFrame before saving
                    if isinstance(deviant_friends, pd.DataFrame) and not deviant_friends.empty:
                            print(f"fetched deviant friends for {deviant}")
                            deviant_friends.to_csv(dev_frnds, mode="a", header=not os.path.exists(dev_frnds), index=False)
                            print(f"Saved {deviant} friends")
                            time.sleep(5) # Add delay after saving friends
                    else:
                            print(f"Empty or invalid deviant friends data for {deviant}")
                           # Clear the list for the next batch
                            user_name = []
                            time.sleep(5) # Add delay even if friends are empty

                    self.refresh_token()
                    time.sleep(random.uniform(10, 20)) # Add a significant delay between processing deviants

                else:
                            print(f"Skipping already visited {deviant}")

        except requests.exceptions.RequestException as e:
            print(f"Exception {e} occurred")
        except Exception as e:
             print(f"An unexpected error occurred during data gathering for deviant {deviant}: {e}")

        # Move the finally block outside the try block that contains the loop
        # finally:
        #     # Save the set of visited deviants to a pickle file
        #     try:
        #         with open(visited_deviants_pickle_path, 'wb') as f:
        #             pickle.dump(visited_deviants, f)
        #         print(f"Saved {len(visited_deviants)} visited deviants to {visited_deviants_pickle_path}")
        #     except Exception as e:
        #         print(f"Error saving visited deviants to pickle file: {e}")


        # Save visited deviants and print completion message after the loop finishes
        try:
            with open(visited_deviants_pickle_path, 'wb') as f:
                pickle.dump(visited_deviants, f)
            print(f"Saved {len(visited_deviants)} visited deviants to {visited_deviants_pickle_path}")
        except Exception as e:
            print(f"Error saving visited deviants to pickle file: {e}")

        print("Gathering of watchers and friends completed.")


# Example usage:
client_id = "42096"
client_secret = "97080792c6d30a4178965e41f1ca15de"
TOKEN_URL = "https://www.deviantart.com/oauth2/token"
REDIRECT_URI = "https://www.deviantart.com/oauth2/authorize"

# Initialize token refresh timer
wtchrs_frnds = DeviantArtWatchersFriends(client_id, client_secret, TOKEN_URL, REDIRECT_URI)
# wtchrs_frnds.get_token() # Called internally by refresh_token
# wtchrs_frnds.refresh_token() # Called internally by gather_watchers_friends
wtchrs_frnds.gather_watchers_friends()