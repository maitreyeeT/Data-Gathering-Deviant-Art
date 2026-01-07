import deviantart
import requests
import random
import sqlite3
from bs4 import BeautifulSoup
import json
import time
import requests
import requests.auth
import datetime
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
import threading
import os, re
import pandas as pd


class DeviantArtRandomWalk:

    def __init__(self, client_id, client_secret): #database_path="/mnt/hdd/maittewa/random_deviants_watchers_watching_testing.db"
        self.da = deviantart.Api(client_id, client_secret)
        #self.database_path = database_path
        #self.create_database()
        #self.conn = sqlite3.connect(self.database_path)
        #self.cursor = self.conn.cursor()

    def get_token(self):
        client = BackendApplicationClient(client_id=client_id)
        oauth = OAuth2Session(client=client)
        token = oauth.fetch_token(token_url=TOKEN_URL, client_id=client_id, client_secret=client_secret)
        # Extract the access token
        access_token = token['access_token']
        return access_token

    def get_random_deviants_from_daily_deviations(self, num_deviants, date):
        """Fetches a list of random deviants from a specific tag and page."""
        token = self.get_token()
        url = f"https://www.deviantart.com/api/v1/oauth2/browse/dailydeviations?access_token={token}"
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

    def get_user_profile(self, username):
        token = self.get_token()
        # url=f"https://www.deviantart.com/api/v1/oauth2/user/profile/{username}?access_token={token}"
        url = f"https://www.deviantart.com/api/v1/oauth2/user/profile/{username}?access_token={token}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error getting profile info: {e}")
            return None
        return response

    def parse_user_profile(self, user):
        s = self.get_user_profile(user)
        d = json.loads(s.text)
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
            return a
            print(f'Gather profile information for the {user}')
        else:
            return 'error'

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

    # Function to get friends
    def get_friends(self, username, page):
        token = self.get_token()
        api_url = f"https://www.deviantart.com/api/v1/oauth2/user/friends/{username}?access_token={token}"
        response = requests.get(api_url, params={'offset': page, 'limit': 50})
        return response

    # Function to get watchers
    def get_watchers(self, username, page):
        token = self.get_token()
        api_url = f"https://www.deviantart.com/api/v1/oauth2/user/watchers/{username}?access_token={token}"
        response = requests.get(api_url, params={'offset': page, 'limit': 50})
        return response

    # Function to parse friends
    def parse_friends(self, friends):
        users = pd.DataFrame()
        # print(friends.keys())
        # next_offset=friends['next_offset']
        has_more = friends.get('has_more')
        for i in friends['results']:
            a = {'username': i['user']['username'],
                 'is_watching': i['is_watching'],
                 'watches_you': i['watches_you'],
                 'last_visit': i['lastvisit'],
                 'activity': i['watch']['activity'],
                 'collections': i['watch']['collections'],
                 'critiques': i['watch']['critiques'],
                 'deviations': i['watch']['deviations'],
                 'forum_threads': i['watch']['forum_threads'],
                 'friend': i['watch']['friend'],
                 'journals': i['watch']['journals'],
                 'scraps': i['watch']['scraps']}
            dict_pd = pd.DataFrame.from_dict(a, orient='index').transpose()
            users = pd.concat([users, dict_pd])
        return has_more, users

    def parse_watchers(self, watchers):
        users = pd.DataFrame()
        # print(friends.keys())
        # next_offset=friends['next_offset']
        has_more = watchers.get('has_more')
        for i in watchers['results']:
            a = {'username': i['user']['username'],
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
        return has_more, users

    def get_watchers_and_watching(self,username):
        """Gathers watchers and watching using API."""

        watchers_pd = pd.DataFrame()
        # Get watching (friends)
        friends_pd = pd.DataFrame()
        has_more = True
        try:
                # Get the initial batch of watchers
                for i in range(0, 5):
                    if has_more == True:
                        resp = self.get_watchers(username, i)
                        watchers = self.get_response_rate(resp)
                        if watchers is not None:
                            has_more, parsed_watchers = self.parse_watchers(watchers)
                            if len(parsed_watchers) > 0:
                                watchers_pd = pd.concat([watchers_pd, parsed_watchers])
                # Get the initial batch of watching users
                for i in range(0, 5):
                    if has_more == True:
                        resp = self.get_friends(username, i)
                        friends = self.get_response_rate(resp)
                        if friends is not None:
                            has_more, parsed_frnds = self.parse_friends(friends)
                            if len(parsed_frnds) > 0:
                                friends_pd = pd.concat([friends_pd, parsed_frnds])


        except requests.exceptions.RequestException as e:
                print(f"Error scraping DeviantArt watchers and watching: {e}")
                print(f"Entering HTTPError handler...")
                print(f"HTTP Error: {e} for user: {username}")
                status_code_match = re.search(r"(\d{3})", str(e))  # Extract status code from error message
                if status_code_match and status_code_match.group(1) == "401":
                    print("401 Unauthorized error. Reauthenticating...")
                    time.sleep(60)
                elif e.response.status_code == 403:
                    print(f"HTTP 403 Error for user {username}. Access forbidden.")
                    time.sleep(60)

                # if e.response.status_code == 401:
                #   print("401 Unauthorized error detected. Reauthenticating...")
                #   reauthenticate()  # Call the reauthentication function
                else:
                    # Handle other HTTP errors
                    print(f"Handling other HTTP errors...")

        return watchers_pd, friends_pd


```python
import deviantart # Imports the deviantart API wrapper
import requests # Used for making HTTP requests
import random # Used for generating random numbers, potentially for delays or sampling
import sqlite3 # Imports SQLite3 for database interaction (currently commented out)
from bs4 import BeautifulSoup # Used for parsing HTML (not directly used in the current version of this class)
import json # Used for handling JSON data
import time # Used for introducing delays
import requests.auth # Imports requests.auth (not directly used in the current version of this class)
import datetime # Imports datetime module (not directly used in the current version of this class)
from requests_oauthlib import OAuth2Session # Part of OAuth2 authentication
from oauthlib.oauth2 import BackendApplicationClient # Part of OAuth2 authentication
import threading # Used for thread-safe operations, like token refreshing
import os # Used for operating system interactions, like file paths (not directly used in the current version of this class)
import re # Used for regular expressions (specifically for error parsing)
import pandas as pd # Used for data manipulation and creating DataFrames


class DeviantArtRandomWalk:
    """
    A class designed to perform a "random walk" data collection on DeviantArt,
    focusing on gathering user profiles, watchers, and friends data using the
    DeviantArt API. Note: Some initial database-related code is commented out.
    """

    def __init__(self, client_id, client_secret):
        """
        Initializes the DeviantArtRandomWalk instance.

        Args:
            client_id (str): The client ID for DeviantArt API authentication.
            client_secret (str): The client secret for DeviantArt API authentication.

        Imports Used:
            deviantart
        """
        # Initialize the DeviantArt API wrapper
        self.da = deviantart.Api(client_id, client_secret)
        # Commented out database initialization. If uncommented, it would set up SQLite.
        # self.database_path = "/mnt/hdd/maittewa/random_deviants_watchers_watching_testing.db"
        # self.create_database()
        # self.conn = sqlite3.connect(self.database_path)
        # self.cursor = self.conn.cursor()
        self.access_token = None # Initialize access token to None
        self.last_token_refresh_time = 0 # Initialize token refresh time
        self.token_lock = threading.Lock() # Initialize a lock for thread-safe token refresh

    def get_token(self):
        """
        Obtains an access token from the DeviantArt API using the client credentials flow.
        This token is essential for making authenticated API requests.

        Returns:
            str: The obtained access token.

        Imports Used:
            requests_oauthlib.OAuth2Session, oauthlib.oauth2.BackendApplicationClient
        """
        # Note: client_id, client_secret, TOKEN_URL are expected to be global or class attributes.
        # The code as written implicitly relies on global variables for TOKEN_URL, client_id, client_secret.
        # For better practice, these should be class attributes initialized in __init__ or passed explicitly.
        client = BackendApplicationClient(client_id=client_id)
        oauth = OAuth2Session(client=client)
        # It's assumed TOKEN_URL, client_id, client_secret are available in the scope.
        token = oauth.fetch_token(token_url=TOKEN_URL, client_id=client_id, client_secret=client_secret)
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

    def get_random_deviants_from_daily_deviations(self, num_deviants, date):
        """
        Fetches a list of random deviants from DeviantArt's daily deviations for a specific date.

        Args:
            num_deviants (int): The number of random deviants to retrieve.
            date (datetime.date): The specific date for which to fetch daily deviations.

        Returns:
            list: A list of usernames of random deviants.

        Imports Used:
            requests, json, random, datetime (for date formatting)
        """
        # Ensure an access token is available
        if not self.access_token:
            self.access_token = self.get_token()

        # The URL for daily deviations API
        url = f"https://www.deviantart.com/api/v1/oauth2/browse/dailydeviations?access_token={self.access_token}"
        params = {
            # client_id and client_secret are redundant here if access_token is already used.
            # "client_id": client_id,
            # "client_secret": client_secret,
            "date": date.strftime("%Y-%m-%d"),  # Format date as YYYY-MM-DD
            # Additional parameters like 'limit' or 'offset' could be added here
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            data = response.json()
            # Extract usernames from the API response
            deviants = [deviation["author"]["username"] for deviation in data["results"]]
            # Select a random sample of deviants, ensuring not to exceed the available number
            random_deviants = random.sample(deviants, min(num_deviants, len(deviants)))
            return random_deviants
        except Exception as e:
            print(f"Error fetching deviants: {e}")
            return []

    def get_user_profile(self, username):
        """
        Fetches the raw user profile data for a given username from the DeviantArt API.

        Args:
            username (str): The username of the deviant.

        Returns:
            requests.Response or None: The response object containing profile data, or None if an error occurs.

        Imports Used:
            requests
        """
        # Ensure an access token is available
        if not self.access_token:
            self.access_token = self.get_token()

        # Construct the API URL for user profile
        url = f"https://www.deviantart.com/api/v1/oauth2/user/profile/{username}?access_token={self.access_token}"
        try:
            response = requests.get(url)
            response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        except requests.exceptions.RequestException as e:
            print(f"Error getting profile info for {username}: {e}")
            return None
        return response

    def parse_user_profile(self, user):
        """
        Parses the raw API response for a user profile into a dictionary.

        Args:
            user (str): The username of the deviant.

        Returns:
            dict or str: A dictionary containing the parsed user profile information, or 'error' if parsing fails.

        Imports Used:
            json
        """
        s = self.get_user_profile(user) # Get the raw profile response
        if s is None: # Handle case where get_user_profile returns None
            return 'error'

        d = json.loads(s.text) # Parse the JSON response
        if 'error' not in d.keys():
            # Extract relevant fields into a dictionary
            a = {'user': user,
                 'real_name': d.get('real_name'),
                 'profil_url': d.get('profile_url'), # Use .get for safety
                 'tags': d.get('tagline'),
                 'country': d.get('countryid'),
                 'bio': d.get('bio'),
                 'level': d.get('artist_level'),
                 'specialty': d.get('artist_specialty')}
            # Update with statistics if available
            if 'stats' in d: # Check if 'stats' key exists
                a.update(d['stats'])
            print(f'Gathered profile information for {user}')
            return a
        else:
            print(f"Error in DeviantArt API response for {user}: {d.get('error_description', d.get('error'))}")
            return 'error'

    def get_response_rate(self, response):
        """
        Processes an HTTP response from the DeviantArt API, handling different status codes.

        Args:
            response (requests.Response): The response object from an API call.

        Returns:
            dict or str: Parsed JSON data for 200 OK, or a status string for specific errors.

        Imports Used:
            json
        """
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
        else:
            return f'unexpected_status_code_{response.status_code}'

    def get_friends(self, username, page):
        """
        Fetches a batch of friends for a given username and page (offset).

        Args:
            username (str): The DeviantArt username.
            page (int): The offset for pagination.

        Returns:
            requests.Response: The response object containing friends data.

        Imports Used:
            requests
        """
        # Ensure an access token is available
        if not self.access_token:
            self.access_token = self.get_token()

        api_url = f"https://www.deviantart.com/api/v1/oauth2/user/friends/{username}?access_token={self.access_token}"
        # The offset parameter for friends API seems to be direct page number multiplied by limit (50)
        response = requests.get(api_url, params={'offset': page * 50, 'limit': 50})
        return response

    def get_watchers(self, username, page):
        """
        Fetches a batch of watchers for a given username and page (offset).

        Args:
            username (str): The DeviantArt username.
            page (int): The offset for pagination.

        Returns:
            requests.Response: The response object containing watchers data.

        Imports Used:
            requests
        """
        # Ensure an access token is available
        if not self.access_token:
            self.access_token = self.get_token()

        api_url = f"https://www.deviantart.com/api/v1/oauth2/user/watchers/{username}?access_token={self.access_token}"
        # The offset parameter for watchers API seems to be direct page number multiplied by limit (50)
        response = requests.get(api_url, params={'offset': page * 50, 'limit': 50})
        return response

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
            a = {'username': i['user']['username'],
                 'is_watching': i['is_watching'],
                 'watches_you': i['watches_you'],
                 'last_visit': i['lastvisit'],
                 'activity': i['watch']['activity'],
                 'collections': i['watch']['collections'],
                 'critiques': i['watch']['critiques'],
                 'deviations': i['watch']['deviations'],
                 'forum_threads': i['watch']['forum_threads'],
                 'friend': i['watch']['friend'],
                 'journals': i['watch']['journals'],
                 'scraps': i['watch']['scraps']}
            dict_pd = pd.DataFrame.from_dict(a, orient='index').transpose()
            users = pd.concat([users, dict_pd], ignore_index=True)
        return has_more, users

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
            a = {'username': i['user']['username'],
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
        return has_more, users

    def get_watchers_and_watching(self, username):
        """
        Gathers watchers and friends (referred to as 'watching') data for a given username using the DeviantArt API.

        It iterates through a fixed number of pages (up to 5) to fetch both watchers and friends.

        Args:
            username (str): The DeviantArt username to fetch data for.

        Returns:
            tuple: A tuple containing two pandas DataFrames:
                   - `watchers_pd`: DataFrame of watchers.
                   - `friends_pd`: DataFrame of friends.
                   Returns (empty_DataFrame, empty_DataFrame) if errors occur.

        Imports Used:
            requests.exceptions.RequestException, pandas, re, time
        """
        watchers_pd = pd.DataFrame()
        friends_pd = pd.DataFrame()
        has_more_watchers = True # Flag to control watcher pagination
        has_more_friends = True # Flag to control friends pagination

        try:
            # Iterate up to 5 pages for both watchers and friends
            for i in range(0, 5):
                # Fetch watchers data
                if has_more_watchers:
                    resp_watchers = self.get_watchers(username, i)
                    watchers_json = self.get_response_rate(resp_watchers)
                    if watchers_json is not None and watchers_json != 'user_done' and watchers_json != 'json_decode_error' and watchers_json != 'server error':
                        has_more_watchers, parsed_watchers = self.parse_watchers(watchers_json)
                        if not parsed_watchers.empty:
                            watchers_pd = pd.concat([watchers_pd, parsed_watchers], ignore_index=True)
                    else:
                        has_more_watchers = False # Stop fetching if no data, user done, or error
                        if watchers_json == 'user_done':
                            print(f"No more watchers found for {username} after page {i}.")
                        elif watchers_json == 'json_decode_error':
                            print(f"JSON decode error for watchers of {username} on page {i}.")
                        elif watchers_json == 'server error':
                            print(f"Server error for watchers of {username} on page {i}.")
                        else:
                            print(f"No watchers JSON data or unexpected response for {username} on page {i}.")

                # Fetch friends data
                if has_more_friends:
                    resp_friends = self.get_friends(username, i)
                    friends_json = self.get_response_rate(resp_friends)
                    if friends_json is not None and friends_json != 'user_done' and friends_json != 'json_decode_error' and friends_json != 'server error':
                        has_more_friends, parsed_frnds = self.parse_friends(friends_json)
                        if not parsed_frnds.empty:
                            friends_pd = pd.concat([friends_pd, parsed_frnds], ignore_index=True)
                    else:
                        has_more_friends = False # Stop fetching if no data, user done, or error
                        if friends_json == 'user_done':
                            print(f"No more friends found for {username} after page {i}.")
                        elif friends_json == 'json_decode_error':
                            print(f"JSON decode error for friends of {username} on page {i}.")
                        elif friends_json == 'server error':
                            print(f"Server error for friends of {username} on page {i}.")
                        else:
                            print(f"No friends JSON data or unexpected response for {username} on page {i}.")

                # Refresh token and introduce a small delay between page fetches
                self.refresh_token()
                time.sleep(random.uniform(1, 3)) # Small random delay to respect API limits

        except requests.exceptions.RequestException as e:
            print(f"Error scraping DeviantArt watchers/friends for {username} due to network/API issue: {e}")
            # Extract status code from error message if available
            status_code_match = re.search(r"(\\d{3})", str(e))
            if status_code_match and status_code_match.group(1) == "401":
                print("401 Unauthorized error. Attempting token refresh.")
                self.refresh_token() # Attempt to refresh token on 401
            elif status_code_match and status_code_match.group(1) == "403":
                print(f"HTTP 403 Forbidden error for user {username}.")
            # No explicit sleep here as retry logic should handle it in get_friends_watchers, if integrated.
        except Exception as e:
            print(f"An unexpected error occurred while fetching friends or watchers for {username}: {e}")

        return watchers_pd, friends_pd

# parameters for instantiating the class (assuming client_id, client_secret, TOKEN_URL, REDIRECT_URI are defined elsewhere)
# client_id = "YOUR_CLIENT_ID"
# client_secret = "YOUR_CLIENT_SECRET"
# TOKEN_URL = "https://www.deviantart.com/oauth2/token"
# REDIRECT_URI = "https://www.deviantart.com/oauth2/authorize"

# # Instantiate the DeviantArtRandomWalk class
# walker = DeviantArtRandomWalk(client_id, client_secret)

# # To check watchers and friends for a user
# watchers_df, friends_df = walker.get_watchers_and_watching("some_deviant_username")
# print("Watchers Data:")
# print(watchers_df.head())
# print("Friends Data:")
# print(friends_df.head())
