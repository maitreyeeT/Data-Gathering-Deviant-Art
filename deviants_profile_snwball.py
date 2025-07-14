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
import watchers_friends_new

class DeviantArtProfileData:

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
                'level': d.get('artist_level'),
                'specialty': d.get('artist_specialty')}
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
        print("Random walk completed.")

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
        prof_new = "/mnt/hdd/maittewa/deviants_profileSnwball_4.csv"
        watcrs_new = "/mnt/hdd/maittewa/deviants_watchersSnwball_2.csv"
        frnds_new = "/mnt/hdd/maittewa/deviants_friendsSnwBall_2.csv"
        class_wat_frnds = watchers_friends_new.DeviantArtWatchersFriends()
        gather_friends_watchers = class_wat_frnds.watchers_friends_data()
        visited_deviants = set(profile['user'])  # Start with deviants in profile_df
        current_round = 0
        while current_round < rounds:
            current_round += 1
            print(f"Starting round {current_round}...")
    
            # Combine usernames from all DataFrames to find new unique deviants
            all_usernames = pd.concat([friends['Deviant'], watchers['Deviant'], profile['user']])
            new_deviants = set(all_usernames) - visited_deviants
    
            if not new_deviants:
                print("No new unique deviants found. Stopping snowball sampling.")
                break
    
            # Gather data for new deviants and update DataFrames
            for deviant in new_deviants:
                profile_data = self.parse_user_profile(deviant)
                if profile_data is not None and not profile_data.empty:
                    print(f"fetched user profile data for {deviant}")
                    profile_data.to_csv(profile_new, mode="a", header=not os.path.exists(profile_new), index=False)
                    print(f"Saved {deviant} profile and the profile data has the shape {profile_data.shape}")
                else:
                    print(f"Empty user profile data for {deviant}")
                time.sleep(3)
                watchers_friends = gather_friends_watchers.watchers_friends_data(deviant)
                if len(watchers_friends) == 2:
                    deviant_watchers, deviant_friends = watchers_friends
                else:
                    print(f"No watchers, friends of {deviant} available")  
                        
                if deviant_watchers is not None and not isinstance(deviant_watchers, list) and not deviant_watchers.empty:  # Save only if not None, not a list and not empty:
                    print(f"fetched deviant watchers for {deviant}")
                    deviant_watchers.to_csv(watcrs_new, mode="a", header=not os.path.exists(dev_watcrs), index=False)
                    print(f"Saved {deviant} watchers")
                    watchers_df = pd.concat([deviant_watchers,watchers_df])
                    print("Concatenated gathered watchers with previous watchers")
                    time.sleep(5)
                else:
                    print(f"Empty deviant watchers for {deviant}")
                    continue
                                
                if deviant_friends is not None and not isinstance(deviant_friends, list) and not deviant_friends.empty:  # Save only if not None, not a list and not empty:
                    print(f"fetched deviant friends for {deviant}")
                    deviant_friends.to_csv(frnds_new, mode="a", header=not os.path.exists(dev_frnds), index=False)
                    print(f"Saved {deviant} friends")
                    deviant_df = pd.concat([deviant_friends,friends_df])
                    print("Concatenated gathered watchers with previous watchers")
                else:
                    print(f"Empty deviant friends for {deviant}")
                profile_data = pd.DataFrame()
                self.refresh_token()
    
        print("Snowball sampling completed.")

#Specify credentials
client_id = "42096"
client_secret = "97080792c6d30a4178965e41f1ca15de"
TOKEN_URL = "https://www.deviantart.com/oauth2/token"
REDIRECT_URI = "https://www.deviantart.com/oauth2/authorize"

profile = pd.read_csv("/mnt/hdd/maittewa/deviants_profileSnwball_3.csv")
watchers = pd.read_csv("/mnt/hdd/maittewa/deviants_watchersSnwball_1.csv")
friends = pd.read_csv("/mnt/hdd/maittewa/deviants_friendsSnwBall_1.csv")
snwball_smplng = DeviantArtProfileData(client_id, client_secret, TOKEN_URL, REDIRECT_URI)
snwball_smplng.get_token()
snwball_smplng.refresh_token()
snwball_smplng.snwball_smplng(profile, watchers, friends)