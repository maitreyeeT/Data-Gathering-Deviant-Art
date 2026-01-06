import requests
import threading
import json
import time
import random
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
import os
import pandas as pd
from requests.exceptions import HTTPError
import pickle


class DeviantArtWatchersFriends:
    """
    A class to interact with the DeviantArt API for gathering watchers and friends data.
    It handles authentication, API requests, rate limiting, and data parsing.
    """

    def __init__(self, client_id, client_secret, TOKEN_URL, REDIRECT_URI):
        """
        Initializes the DeviantArtWatchersFriends instance.

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

    def get_response_rate(self, response):
        """
        Processes an HTTP response from the DeviantArt API, handling different status codes.

        Args:
            response (requests.Response or None): The response object from an API call.

        Returns:
            dict or str or None: Parsed JSON data for 200 OK, a status string for specific errors,
                                  or None if the input response is None.

        Imports Used:
            json
        """
        if response is None:
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
        """
        Fetches a batch of friends and watchers for a given username and page (offset).
        Includes retry logic with exponential backoff for rate limiting (429 errors).

        Args:
            username (str): The DeviantArt username.
            page (int): The offset for pagination (multiplied by limit inside the function).

        Returns:
            tuple: A tuple containing the response objects for watchers and friends, or (None, None) if an error persists.

        Imports Used:
            requests, time, random
        """
        max_retries = 5
        limit = 50 # Number of items per page

        # Check if we have a valid token, get one if not
        if not self.access_token:
            self.access_token = self.get_token()

        if self.access_token:
            # Construct API URLs with the current access token
            api_url_friends = f"https://www.deviantart.com/api/v1/oauth2/user/friends/{username}?access_token={self.access_token}"
            api_url_watchers = f"https://www.deviantart.com/api/v1/oauth2/user/watchers/{username}?access_token={self.access_token}"

            for retry_count in range(max_retries):
                try:
                    # Make requests for watchers and friends with the calculated offset
                    response_watchers = requests.get(api_url_watchers, params={'offset': page * limit, 'limit': limit})
                    response_friends = requests.get(api_url_friends, params={'offset': page * limit, 'limit': limit})

                    # Check for rate limit error specifically
                    if response_watchers.status_code == 429 or response_friends.status_code == 429:
                        wait_time = 2 ** retry_count + random.uniform(5, 10) # Exponential backoff with jitter
                        print(f"Rate limit hit. Retrying in {wait_time:.2f} seconds (Attempt {retry_count + 1}/{max_retries}).")
                        time.sleep(wait_time)
                        self.refresh_token() # Consider refreshing token on 429 as well
                        continue # Retry the request

                    # Check for other potential non-200 status codes before raising for status
                    if response_watchers.status_code != 200 or response_friends.status_code != 200:
                        print(f"Non-200 status code received for {username} (offset {page * limit}, limit {limit}). Watchers Status: {response_watchers.status_code}, Friends Status: {response_friends.status_code}")
                        # Return the responses even if not 200, so get_response_rate can handle them
                        return (response_watchers, response_friends)

                    # Raise HTTPError for other bad responses (if not handled above)
                    response_watchers.raise_for_status()
                    response_friends.raise_for_status()

                    return (response_watchers, response_friends)

                except requests.exceptions.RequestException as e:
                    print(f"Error getting info for {username} (offset {page * limit}, limit {limit}): {e}")
                    if retry_count < max_retries - 1:
                        wait_time = 2 ** retry_count + random.uniform(0, 1)
                        print(f"Request failed. Retrying in {wait_time:.2f} seconds (Attempt {retry_count + 1}/{max_retries}).")
                        time.sleep(wait_time)
                        self.refresh_token() # Refresh token before retrying
                    else:
                        print(f"Max retries ({max_retries}) reached for {username} (offset {page * limit}, limit {limit}). Giving up.")
                        return None, None # Return None after max retries

            return None, None # Return None if loop finishes without success
        return None, None # Return None if access token is not available


    def parse_friends(self, friends_data):
        """
        Parses the raw JSON data for friends into a pandas DataFrame.

        Args:
            friends_data (dict): The JSON response containing friends information.

        Returns:
            tuple: A tuple containing a boolean indicating if there are more friends (has_more) and
                   a pandas DataFrame of parsed friends data.

        Imports Used:
            pandas
        """
        users = pd.DataFrame()
        has_more = friends_data.get('has_more')
        for i in friends_data['results']:
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
            users = pd.concat([users, dict_pd], ignore_index=True)
        return (has_more, users)

    def parse_watchers(self, watchers_data):
        """
        Parses the raw JSON data for watchers into a pandas DataFrame.

        Args:
            watchers_data (dict): The JSON response containing watchers information.

        Returns:
            tuple: A tuple containing a boolean indicating if there are more watchers (has_more) and
                   a pandas DataFrame of parsed watchers data.

        Imports Used:
            pandas
        """
        users = pd.DataFrame()
        has_more = watchers_data.get('has_more')
        for i in watchers_data['results']:
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
        """
        Gathers all available watchers and friends data for a given username by iteratively
        fetching and parsing API responses.

        Args:
            username (str): The DeviantArt username to fetch data for.

        Returns:
            tuple: A tuple containing two pandas DataFrames: one for watchers and one for friends.
                   Returns (empty_DataFrame, empty_DataFrame) if no data or an error occurs.

        Imports Used:
            requests.exceptions.RequestException, pandas
        """
        watchers_pd = pd.DataFrame()
        friends_pd = pd.DataFrame()
        has_more_watchers = True
        has_more_friends = True
        page = 0

        try:
            # Continue fetching as long as there might be more data for either watchers or friends
            while (has_more_watchers or has_more_friends) and page < 10: # Limit to 10 pages to avoid excessive calls
                resp_watchers, resp_friends = self.get_friends_watchers(username, page)

                watchers_json = self.get_response_rate(resp_watchers)
                friends_json = self.get_response_rate(resp_friends)

                # Process watchers data
                if watchers_json is not None and watchers_json != 'user_done' and watchers_json != 'json_decode_error':
                    has_more_watchers, parsed_watchers = self.parse_watchers(watchers_json)
                    if not parsed_watchers.empty:
                        parsed_watchers["Deviant"] = username
                        watchers_pd = pd.concat([watchers_pd, parsed_watchers], ignore_index=True)
                else:
                    has_more_watchers = False # Stop fetching watchers if no data or error
                    if watchers_json == 'user_done':
                        print(f"No more watchers found for {username} after page {page}.")
                    elif watchers_json == 'json_decode_error':
                        print(f"JSON decode error for watchers of {username} on page {page}.")
                    else:
                        print(f"No watchers JSON data or unexpected response for {username} on page {page}.")

                # Process friends data
                if friends_json is not None and friends_json != 'user_done' and friends_json != 'json_decode_error':
                    has_more_friends, parsed_frnds = self.parse_friends(friends_json)
                    if not parsed_frnds.empty:
                        parsed_frnds["Deviant"] = username
                        friends_pd = pd.concat([friends_pd, parsed_frnds], ignore_index=True)
                else:
                    has_more_friends = False # Stop fetching friends if no data or error
                    if friends_json == 'user_done':
                        print(f"No more friends found for {username} after page {page}.")
                    elif friends_json == 'json_decode_error':
                        print(f"JSON decode error for friends of {username} on page {page}.")
                    else:
                        print(f"No friends JSON data or unexpected response for {username} on page {page}.")

                page += 1
                self.refresh_token()
                time.sleep(random.uniform(3, 7)) # Small delay between page fetches to respect API limits

        except requests.exceptions.RequestException as e:
            print(f"Error scraping DeviantArt watchers/friends for {username} due to network/API issue: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while fetching friends or watchers for {username}: {e}")

        return watchers_pd, friends_pd

    def gather_watchers_friends(self):
        """
        Orchestrates the gathering of watchers and friends data for a list of unique deviants.
        It loads a list of deviants, iterates through them, fetches their connections,
        and saves the data to CSV files. It also uses a pickle file to track visited deviants
        for resuming interrupted processes.

        Imports Used:
            pandas, os, pickle, time, random
        """
        visited_deviants = set()
        deviant_count = 0

        # Load a list of unique deviants to process
        # Assuming 'Author_Name' column contains the deviant usernames
        unique_deviants_df = pd.read_csv("/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_metaDataSnwBall/uniqueDev_metaData_SnwBall_02.csv.gz")
        unique_dev_list = set(unique_deviants_df["Author_Name"].tolist())

        # Define output file paths
        dev_watcrs_path = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_watchersSnwball_4/metaDev_watchers-11-07-2025.csv.gz"
        dev_frnds_path = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_friendsSnwBall_1-4/metaDev_friends-11-07-2025.csv.gz"

        # Define the path for the pickle file to store visited deviants
        visited_deviants_pickle_path = "visited_deviants_watchers_friends.pkl"

        # Load visited deviants from the pickle file if it exists to resume progress
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
                if deviant not in visited_deviants:
                    deviant_count += 1
                    visited_deviants.add(deviant)
                    print(f"Fetching watchers and friends data for unique deviant: {deviant} (Count: {deviant_count})")

                    # Fetch watchers and friends data using the dedicated method
                    deviant_watchers, deviant_friends = self.watchers_friends_data(deviant)

                    # Save watchers data if available and not empty
                    if isinstance(deviant_watchers, pd.DataFrame) and not deviant_watchers.empty:
                        print(f"Fetched deviant watchers for {deviant}")
                        deviant_watchers.to_csv(dev_watcrs_path, mode="a", header=not os.path.exists(dev_watcrs_path), index=False)
                        print(f"Saved {deviant} watchers to {dev_watcrs_path}")
                    else:
                        print(f"Empty or invalid deviant watchers data for {deviant}")

                    # Save friends data if available and not empty
                    if isinstance(deviant_friends, pd.DataFrame) and not deviant_friends.empty:
                        print(f"Fetched deviant friends for {deviant}")
                        deviant_friends.to_csv(dev_frnds_path, mode="a", header=not os.path.exists(dev_frnds_path), index=False)
                        print(f"Saved {deviant} friends to {dev_frnds_path}")
                    else:
                        print(f"Empty or invalid deviant friends data for {deviant}")

                    # Refresh token and introduce a delay to respect API rate limits
                    self.refresh_token()
                    time.sleep(random.uniform(10, 20)) # Add a significant delay between processing deviants

                else:
                    print(f"Skipping already visited {deviant}")

        except requests.exceptions.RequestException as e:
            print(f"Exception occurred during data gathering: {e}")
        except Exception as e:
             print(f"An unexpected error occurred during data gathering for deviant {deviant}: {e}")

        finally:
            # Always save the set of visited deviants to the pickle file
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

# Instantiate the DeviantArtWatchersFriends class
wtchrs_frnds = DeviantArtWatchersFriends(client_id, client_secret, TOKEN_URL, REDIRECT_URI)

# Start the data gathering process
wtchrs_frnds.gather_watchers_friends()
