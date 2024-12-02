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
