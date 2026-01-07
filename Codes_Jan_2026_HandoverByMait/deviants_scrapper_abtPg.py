import re
import deviantart
from bs4 import BeautifulSoup
import requests
import time, random


class DeviantArtScraper:
    """
    A class to scrape information from DeviantArt user profiles, including
    'About' page statistics and gallery item links.
    """

    def __init__(self, client_id, client_secret):
        """
        Initializes the DeviantArtScraper with API credentials.

        Args:
            client_id (str): The client ID for DeviantArt API authentication.
            client_secret (str): The client secret for DeviantArt API authentication.

        Imports Used:
            deviantart
        """
        self.da = deviantart.Api(client_id, client_secret)

    def reauthenticate(self):
        """
        Re-authenticates or attempts to refresh the token for the DeviantArt API client.

        This method checks the authorization status of the internal `deviantart.Api`
        object and attempts to refresh the token or initiate the authorization flow
        if not authorized.

        Imports Used:
            None (relies on internal `self.da` object)
        """
        try:
            if self.da.is_authorized:
                # In a real scenario, you might call a specific refresh method if available
                print("API client is authorized. Token assumed to be active or refreshed internally.")
            else:
                print("API client not authorized. Initiating authorization flow...")
                self.da.authorize()  # Initiate authorization flow if not authorized
                print("Authorization successful.")
        except Exception as e:
            print(f"Reauthentication failed: {e}")
            # Consider re-raising the exception if reauthentication is critical:
            # raise

    def scrape(self, username):
        """
        Scrapes the 'About' page and 'Gallery' page of a given DeviantArt username
        to extract user statistics and gallery item links.

        Args:
            username (str): The DeviantArt username to scrape.

        Returns:
            tuple: A tuple containing:
                - dict: `statistics_data` with parsed user statistics.
                - str: `groupmem` containing text about group memberships.
                - str: `badges` containing text about user badges.
                - str: `about_devts` containing general 'about' section text.

            Returns None if a requests.exceptions.RequestException occurs.

        Imports Used:
            requests, BeautifulSoup (from bs4), re
        """
        try:
            # --- Scrape About Page ---
            about_page_url = f"https://www.deviantart.com/{username}/about"
            about_page_response = requests.get(about_page_url)
            about_page_response.raise_for_status()  # Raise an exception for bad HTTP responses (4xx or 5xx)

            about_page_soup = BeautifulSoup(about_page_response.content, "html.parser")
            
            # Regex pattern to find module IDs like 'module-1234567890'
            id_pattern = re.compile(r"module-\d{10}")
            
            # Extract text from various sections of the 'About' page
            userstats_element = about_page_soup.find(id="userstats")  # Element containing user statistics
            group_element = about_page_soup.find(id="group_list_members") # Element containing group memberships
            badges_element = about_page_soup.find(id="badges_activity") # Element containing badges activity
            about_deviant = about_page_soup.find(id=id_pattern) # Element containing general about text

            # Get stripped text or empty string if element not found
            userstats = userstats_element.text.strip() if userstats_element else ""
            groupmem = group_element.text.strip() if group_element else ""
            badges = badges_element.text.strip() if badges_element else ""
            about_devts = about_deviant.text.strip() if about_deviant else ""

            # --- Scrape Gallery Page ---
            gallery_url = f"https://www.deviantart.com/{username}/gallery/"
            gallery_response = requests.get(gallery_url)
            gallery_response.raise_for_status()

            gallery_soup = BeautifulSoup(gallery_response.content, "html.parser")

            # Extract gallery item links (adjust the selector based on the actual page structure)
            gallery_items = gallery_soup.select(".torpedo-thumb-link")  # Example selector for links
            gallery_links = [item["href"] for item in gallery_items] # Extract href attribute from found elements
            # Note: gallery_links are extracted but not returned by this specific method implementation.
            # If needed, add them to the return tuple or process them further.

            statistics_data = {}

            # Define regex patterns for each statistic to extract from the 'userstats' text
            patterns_stats = {
                "Pageviews": r"Pageviews(\d+)",
                "Deviations": r"Deviations(\d+)",
                "Watchers": r"Watchers(\d+)",
                "Watching": r"Watching(\d+)",
                "Favourites": r"Favourites(\d+)",
                "Comments Made": r"Comments\sMade(\d+)",  # Handle space in "Comments Made"
                "Comments Received": r"Comments\sReceived(\d+)",  # Handle space in "Comments Received"
            }

            # Extract values using defined patterns
            for key, pattern in patterns_stats.items(): # Corrected from 'patterns' to 'patterns_stats'
                match = re.search(pattern, userstats)
                if match:
                    statistics_data[key] = int(match.group(1))  # Convert extracted value to integer

            return statistics_data, groupmem, badges, about_devts


        except requests.exceptions.RequestException as e:
            print(f"Error scraping DeviantArt profile for {username}: {e}")
            return None # Return None if a request error occurs
