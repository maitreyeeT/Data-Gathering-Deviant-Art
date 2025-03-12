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


class DeviantArtSnowballDataGathering:

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
        
    def get_profile(self,username):
        if not self.access_token:
            self.access_token = self.get_token()
        if self.access_token:
            try:
                url=f"https://www.deviantart.com/api/v1/oauth2/user/profile/{username}?access_token={self.access_token}"
                response = requests.get(url)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Error getting profile info: {e}")
                return None
            return response

    def parse_user_profile(self,user):
        s = self.get_profile(user)
        profile_df = pd.DataFrame()
        if s is not None:
            d = json.loads(s.text)
            if 'error' not in d.keys():
                a = {'user': user,
                'user_id': d.get('userid'),
                'user_type': d.get("type"),     
                'real_name': d.get('real_name'),
                'profil_url': d.get('profile_url'),
                'tag_line': d.get('tagline'),
                'country': d.get('country'),
                'user_is_artist': d.get('user_is_artist'),
                'website': d.get('website'),
                'bio': d.get('bio'),
                'cover_photo': d.get('cover_photo'),
                'last_status': d.get('last_status'),     
                'bio': d['bio'],
                'level': d['artist_level'],
                'specialty': d['artist_specialty']}
                a.update(d['stats'])
                dict_pd = pd.DataFrame.from_dict(a, orient='index').transpose()
                profile_df = pd.concat([profile_df, dict_pd]) 
                return profile_df
                print(f'Gathered profile information for the {user}')
            else:
                return pd.DataFrame()  

    def append_unique_usernames(self, df1, df2, df3, otpt_df):
        """
        Compares usernames in three DataFrames and appends unique usernames to a separate DataFrame.
    
        Args:
            df1, df2, df3: The three DataFrames to compare.
            output_df: The DataFrame to append the unique usernames to.
        """
    
        # Combine usernames from all three DataFrames
        all_usernames = pd.concat([df1["user"], df2["Deviant"], df3["Deviant"]])
    
        # Get unique usernames
        unique_usernames = all_usernames.unique()
    
        # Create a DataFrame for unique usernames
        unique_usernames_df = pd.DataFrame({"unique_dev": unique_usernames})
    
        # Append unique usernames to output_df
        otpt_df = pd.concat([otpt_df, unique_usernames_df], ignore_index=True)
    
        return otpt_df
    # Function to get friends
    def get_friends_watchers(self, username, page):
        headers = {"Authorization": f"Bearer {self.access_token}"}
        params_gallery = {"username": username}
        # Check if we have a valid token
        if not self.access_token:
            self.access_token = self.get_token()
        if self.access_token:
            try:
                api_url_friends = f"https://www.deviantart.com/api/v1/oauth2/user/friends/{username}?access_token={self.access_token}"
                api_url_watchers = f"https://www.deviantart.com/api/v1/oauth2/user/watchers/{username}?access_token={self.access_token}"
                
                response_watchers = requests.get(api_url_watchers, params={'offset': page, 'limit': 50})
                response_friends = requests.get(api_url_friends, params={'offset': page, 'limit': 50})

                return (response_watchers, response_friends)

            except requests.exceptions.RequestException as e:
                print(f"Error getting info: {e}")
                return None

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
                # Get the initial batch of watchers
                for i in range(0, 5):
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
        
    def fetch_deviant_profile(self):
        """Fetches unique deviants profile from watchers, friends and already gathered deviants."""
        profile = pd.read_csv("/mnt/hdd/maittewa/deviants_profileRndmWalk03.csv")
        watchers = pd.read_csv("/mnt/hdd/maittewa/deviants_watchersRndmWalk03_2.csv")
        friends = pd.read_csv("/mnt/hdd/maittewa/deviants_friendsRndmWalk03_2.csv")
        profile_new = "/mnt/hdd/maittewa/deviants_profileSnwball_3.csv"
        already_existing_profile = pd.read_csv(profile_new)
        deviant_count = 0
        visited_deviants = set()
        unique_deviants = pd.DataFrame()
        visited_deviants.update(set(already_existing_profile["user"].tolist()))
        profile_data = pd.DataFrame()
        unique_deviants_to_gather = self.append_unique_usernames(profile,watchers,friends,unique_deviants)
        unique_dev_list = unique_deviants_to_gather["unique_dev"].tolist()
        for deviant in unique_dev_list:
            #print(f"length of unique deviants ")
            if deviant not in visited_deviants:
                deviant_count += 1
                visited_deviants.add(deviant)
                print(f"Gathering profile data for {deviant}, count {deviant_count}")
                profile_data = self.parse_user_profile(deviant)
                time.sleep(3)
                if profile_data is not None and not profile_data.empty:
                        print(f"fetched user profile data for {deviant}")
                        profile_data.to_csv(profile_new, mode="a", header=not os.path.exists(profile_new), index=False)
                        print(f"Saved {deviant} profile and the profile data has the shape {profile_data.shape}")
                else:
                    print(f"Empty user profile data for {deviant}")
                profile_data = pd.DataFrame()
                self.refresh_token()
            else:
                    print(f"Skipping already visited {deviant}")
        print("Snowball sampled user profile data gtahering completed.")

    def snowball_sampling(self, profile, watchers, friends, rounds=100):
            """
            Performs snowball sampling to gather deviant data.
        
            Args:
                friends_df: DataFrame containing friends data.
                watchers_df: DataFrame containing watchers data.
                profile_df: DataFrame containing profile data.
                rounds: Maximum number of sampling rounds.
        
            Returns:
                A tuple containing the updated friends, watchers, and profile DataFrames.
            """
            """Fetches unique deviants profile from watchers, friends and already gathered deviants."""
            prof_new = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviants_profileSnwball_5.csv"
            watcrs_new = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviants_watchersSnwball_3.csv"
            frnds_new = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviants_friendsSnwBall_3.csv"
            visited_deviants = set(profile['user'])  # Start with deviants in profile_df
            current_round = 0
            deviant_no = 0
            while current_round < rounds:
                current_round += 1
                print(f"Starting round {current_round}...")
        
                # Combine usernames from all DataFrames to find new unique deviants
                all_usernames = pd.concat([friends['Friends name'], watchers['Watchers name'], profile['user']])
                new_deviants = set(all_usernames) - visited_deviants
                print(f"Total number of new deviants: {len(new_deviants)}")
        
                if not new_deviants:
                    print("No new unique deviants found. Stopping snowball sampling.")
                    break
        
                # Gather data for new deviants and update DataFrames
                for deviant in new_deviants:
                        profile_data = self.parse_user_profile(deviant)
                        if profile_data is not None and not profile_data.empty:
                            print(f"fetched user profile data for {deviant}")
                            profile_data.to_csv(prof_new, mode="a", header=not os.path.exists(prof_new), index=False)
                            print(f"Saved {deviant} profile and the profile data has the shape {profile_data.shape}")
                            profile_data = pd.concat([profile, profile_data])
                            print("Concatenated gathered profile with previous profiles")
                        else:
                            print(f"Empty user profile data for {deviant}")
                        time.sleep(3)
                        watchers_friends = self.watchers_friends_data(deviant)
                        if len(watchers_friends) == 2:
                            deviant_watchers, deviant_friends = watchers_friends
                        else:
                            print(f"No watchers, friends of {deviant} available")  
                                
                        if deviant_watchers is not None and not isinstance(deviant_watchers, list) and not deviant_watchers.empty:  # Save only if not None, not a list and not empty:
                            print(f"fetched deviant watchers for {deviant}")
                            deviant_watchers.to_csv(watcrs_new, mode="a", header=not os.path.exists(watcrs_new), index=False)
                            print(f"Saved {deviant} watchers")
                            watchers = pd.concat([watchers, deviant_watchers])
                            print("Concatenated gathered watchers with previous watchers")
                            time.sleep(5)
                        else:
                            print(f"Empty deviant watchers for {deviant}")
                            continue
                                        
                        if deviant_friends is not None and not isinstance(deviant_friends, list) and not deviant_friends.empty:  # Save only if not None, not a list and not empty:
                            print(f"fetched deviant friends for {deviant}")
                            deviant_friends.to_csv(frnds_new, mode="a", header=not os.path.exists(frnds_new), index=False)
                            print(f"Saved {deviant} friends")
                            friends = pd.concat([friends,deviant_friends])
                            print("Concatenated gathered friends with previous friends")
                        else:
                            print(f"Empty deviant friends for {deviant}")
                            
                        visited_deviants.update(new_deviants) 
                        self.refresh_token()
        
            print("Snowball sampling completed.")
            return friends, watchers, profile


#Specify credentials
client_id = "42096"
client_secret = "97080792c6d30a4178965e41f1ca15de"
TOKEN_URL = "https://www.deviantart.com/oauth2/token"
REDIRECT_URI = "https://www.deviantart.com/oauth2/authorize"

profile = pd.read_csv("/mnt/hdd/maittewa/deviantArt_DeviantData/deviants_profileSnwball_3.csv")
watchers = pd.read_csv("/mnt/hdd/maittewa/deviantArt_DeviantData/deviants_watchersSnwball_1.csv")
friends = pd.read_csv("/mnt/hdd/maittewa/deviantArt_DeviantData/deviants_friendsSnwBall_1.csv")
snwball_smplng = DeviantArtSnowballDataGathering(client_id, client_secret, TOKEN_URL, REDIRECT_URI)
snwball_smplng.get_token()
snwball_smplng.refresh_token()
snwball_smplng.snowball_sampling(profile, watchers, friends)