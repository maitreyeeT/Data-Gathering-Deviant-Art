import requests
import threading
import json
import time
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
import os
import pandas as pd
from requests.exceptions import HTTPError
import watchers_friends_new

class DeviantArtProfileData:
    """
    A class to interact with the DeviantArt API for gathering user profile,
    watchers, and friends data, implementing a snowball sampling approach.
    """

    def __init__(self, client_id, client_secret, TOKEN_URL, REDIRECT_URI):
        """
        Initializes the DeviantArtProfileData instance.

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
        self.access_token = None # Initialize access token

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
            # Check if token has expired (20 minutes = 1200 seconds)
            if time.time() - self.last_token_refresh_time > 20 * 60:
                self.access_token = self.get_token()
                self.last_token_refresh_time = time.time()  # Update refresh time
                print("Token refreshed.")

    def get_profile(self, username):
        """
        Fetches the raw user profile data for a given username from the DeviantArt API.

        Args:
            username (str): The username of the deviant.

        Returns:
            requests.Response or None: The response object containing profile data, or None if an error occurs.

        Imports Used:
            requests, requests.exceptions.HTTPError
        """
        if not self.access_token:
            self.access_token = self.get_token()
        if self.access_token:
            try:
                url = f"https://www.deviantart.com/api/v1/oauth2/user/profile/{username}?access_token={self.access_token}"
                response = requests.get(url)
                response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
            except requests.exceptions.RequestException as e:
                print(f"Error getting profile info: {e}")
                return None
            return response
        return None

    def parse_user_profile(self, user):
        """
        Parses the raw API response for a user profile into a pandas DataFrame.

        Args:
            user (str): The username of the deviant.

        Returns:
            pandas.DataFrame: A DataFrame containing the parsed user profile information, or an empty DataFrame if no data.

        Imports Used:
            json, pandas
        """
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
                print(f'Gathered profile information for {user}')
                return profile_df
            else:
                print(f"Error in DeviantArt API response for {user}: {d.get('error_description', d.get('error'))}")
                return pd.DataFrame()
        return pd.DataFrame()

    def append_unique_usernames(self, df1, df2, df3, otpt_df):
        """
        Compares usernames in three DataFrames (profile, watchers, friends) and
        appends unique usernames to a separate output DataFrame.

        Args:
            df1 (pandas.DataFrame): The first DataFrame (e.g., existing profiles).
            df2 (pandas.DataFrame): The second DataFrame (e.g., watchers).
            df3 (pandas.DataFrame): The third DataFrame (e.g., friends).
            otpt_df (pandas.DataFrame): The DataFrame to append the unique usernames to.

        Returns:
            pandas.DataFrame: The output DataFrame with appended unique usernames.

        Imports Used:
            pandas
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
        """
        Fetches unique deviants' profiles from initial watcher, friends, and
        already gathered deviant lists. It iterates through these deviants,
        fetches their profiles, and saves them to a CSV file if they haven't
        been visited yet.

        Imports Used:
            pandas, os, time
        """
        profile = pd.read_csv("/mnt/hdd/maittewa/deviants_profileRndmWalk03.csv")
        watchers = pd.read_csv("/mnt/hdd/maittewa/deviants_watchersRndmWalk03_2.csv")
        friends = pd.read_csv("/mnt/hdd/maittewa/deviants_friendsRndmWalk03_2.csv")
        profile_new = "/mnt/hdd/maittewa/deviants_profileSnwball_3.csv"
        
        # Ensure the output file exists or create it with headers if not
        if not os.path.exists(profile_new):
            # Create an empty DataFrame to get headers for the first write
            empty_profile_df = self.parse_user_profile("dummy_user_for_headers") # Use a dummy to get structure
            if not empty_profile_df.empty:
                empty_profile_df.iloc[0:0].to_csv(profile_new, mode="w", header=True, index=False)
            else:
                print("Could not get profile headers. Skipping initial file creation.")

        already_existing_profile = pd.read_csv(profile_new)
        deviant_count = 0
        visited_deviants = set()
        unique_deviants = pd.DataFrame()
        visited_deviants.update(set(already_existing_profile["user"].tolist()))
        profile_data = pd.DataFrame()
        unique_deviants_to_gather = self.append_unique_usernames(profile, watchers, friends, unique_deviants)
        unique_dev_list = unique_deviants_to_gather["unique_dev"].tolist()

        print(f"Total unique deviants to consider: {len(unique_dev_list)}")

        for deviant in unique_dev_list:
            if deviant not in visited_deviants:
                deviant_count += 1
                visited_deviants.add(deviant)
                print(f"Gathering profile data for {deviant}, count {deviant_count}")
                profile_data = self.parse_user_profile(deviant)
                time.sleep(3)
                if profile_data is not None and not profile_data.empty:
                    print(f"Fetched user profile data for {deviant}")
                    profile_data.to_csv(profile_new, mode="a", header=not os.path.exists(profile_new), index=False)
                    print(f"Saved {deviant} profile and the profile data has the shape {profile_data.shape}")
                else:
                    print(f"Empty or invalid user profile data for {deviant}")
                profile_data = pd.DataFrame() # Clear for next iteration
                self.refresh_token()
            else:
                print(f"Skipping already visited {deviant}")
        print("Random walk completed.")

    def snowball_sampling(self, profile, watchers, friends, rounds=100):
        """
        Performs snowball sampling to iteratively gather deviant data (profiles,
        watchers, and friends). It starts with an initial set of deviants and
        expands by fetching data for their connections.

        Args:
            profile (pandas.DataFrame): Initial DataFrame containing profile data.
            watchers (pandas.DataFrame): Initial DataFrame containing watchers data.
            friends (pandas.DataFrame): Initial DataFrame containing friends data.
            rounds (int): Maximum number of sampling rounds to perform.

        Imports Used:
            pandas, os, time, watchers_friends_new
        """
        prof_new = "/mnt/hdd/maittewa/deviants_profileSnwball_4.csv"
        watcrs_new = "/mnt/hdd/maittewa/deviants_watchersSnwball_2.csv"
        frnds_new = "/mnt/hdd/maittewa/deviants_friendsSnwBall_2.csv"

        # Initialize or load existing data for snowball sampling output files
        # Ensure headers are written only once if files are new
        for filepath in [prof_new, watcrs_new, frnds_new]:
            if not os.path.exists(filepath):
                # For profile, use a dummy call to get header structure
                if filepath == prof_new:
                    dummy_df = self.parse_user_profile("dummy_user_for_headers")
                    if not dummy_df.empty:
                        dummy_df.iloc[0:0].to_csv(filepath, mode="w", header=True, index=False)
                # For watchers and friends, we need to know their structure or create an empty one
                # This part assumes watchers_friends_new.DeviantArtWatchersFriends() can provide headers
                # or that the first write will handle it if the data is not empty.
                # For now, we'll let `to_csv` handle `header=not os.path.exists(filepath)`
                print(f"Created empty file: {filepath}")

        class_wat_frnds = watchers_friends_new.DeviantArtWatchersFriends() # Assuming this class takes no args or handles it internally

        # Use sets for efficient lookup of visited deviants
        visited_deviants = set(profile['user'].tolist()) if 'user' in profile.columns else set()

        current_round = 0
        while current_round < rounds:
            current_round += 1
            print(f"Starting round {current_round}...")

            # Combine usernames from all DataFrames to find new unique deviants
            all_usernames = pd.concat([friends['Deviant'], watchers['Deviant'], profile['user']])
            new_deviants = set(all_usernames.unique()) - visited_deviants

            if not new_deviants:
                print("No new unique deviants found. Stopping snowball sampling.")
                break

            print(f"Found {len(new_deviants)} new deviants to process in this round.")

            # Gather data for new deviants and update DataFrames
            for deviant in list(new_deviants): # Convert set to list to iterate
                print(f"Processing new deviant: {deviant}")
                visited_deviants.add(deviant)

                # Fetch profile data
                profile_data = self.parse_user_profile(deviant)
                if profile_data is not None and not profile_data.empty:
                    print(f"Fetched user profile data for {deviant}")
                    profile_data.to_csv(prof_new, mode="a", header=not os.path.exists(prof_new), index=False)
                    profile = pd.concat([profile, profile_data], ignore_index=True) # Update in-memory profile DF
                    print(f"Saved {deviant} profile and updated in-memory profile DF (shape: {profile.shape})")
                else:
                    print(f"Empty or invalid user profile data for {deviant}")
                time.sleep(3)

                # Fetch watchers and friends data
                # Assuming watchers_friends_new.DeviantArtWatchersFriends().watchers_friends_data() takes a deviant name
                watchers_friends_result = class_wat_frnds.watchers_friends_data(deviant)

                deviant_watchers = None
                deviant_friends = None

                if watchers_friends_result and len(watchers_friends_result) == 2:
                    deviant_watchers, deviant_friends = watchers_friends_result
                else:
                    print(f"No watchers/friends data or unexpected format for {deviant}")

                if deviant_watchers is not None and not isinstance(deviant_watchers, list) and not deviant_watchers.empty:  # Save only if not None, not a list and not empty:
                    print(f"Fetched deviant watchers for {deviant}")
                    deviant_watchers.to_csv(watcrs_new, mode="a", header=not os.path.exists(watcrs_new), index=False)
                    watchers = pd.concat([watchers, deviant_watchers], ignore_index=True) # Update in-memory watchers DF
                    print(f"Saved {deviant} watchers and updated in-memory watchers DF (shape: {watchers.shape})")
                    time.sleep(5)
                else:
                    print(f"Empty or invalid deviant watchers for {deviant}")

                if deviant_friends is not None and not isinstance(deviant_friends, list) and not deviant_friends.empty:  # Save only if not None, not a list and not empty:
                    print(f"Fetched deviant friends for {deviant}")
                    deviant_friends.to_csv(frnds_new, mode="a", header=not os.path.exists(frnds_new), index=False)
                    friends = pd.concat([friends, deviant_friends], ignore_index=True) # Update in-memory friends DF
                    print(f"Saved {deviant} friends and updated in-memory friends DF (shape: {friends.shape})")
                else:
                    print(f"Empty or invalid deviant friends for {deviant}")

                self.refresh_token() # Refresh token after a few API calls
                time.sleep(3) # Small delay to respect API rate limits

        print("Snowball sampling completed.")

# Specify credentials
client_id = "42096"
client_secret = "97080792c6d30a4178965e41f1ca15de"
TOKEN_URL = "https://www.deviantart.com/oauth2/token"
REDIRECT_URI = "https://www.deviantart.com/oauth2/authorize"

# Load initial data for snowball sampling
profile = pd.read_csv("/mnt/hdd/maittewa/deviants_profileSnwball_3.csv")
watchers = pd.read_csv("/mnt/hdd/maittewa/deviants_watchersSnwball_1.csv")
friends = pd.read_csv("/mnt/hdd/maittewa/deviants_friendsSnwBall_1.csv")

# Instantiate the DeviantArtProfileData class
snwball_smplng = DeviantArtProfileData(client_id, client_secret, TOKEN_URL, REDIRECT_URI)

# Get initial token and refresh it
snwball_smplng.get_token()
snwball_smplng.refresh_token()

# Start snowball sampling
snwball_smplng.snowball_sampling(profile, watchers, friends)
