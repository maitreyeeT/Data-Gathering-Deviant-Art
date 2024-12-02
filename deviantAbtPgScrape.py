import re
import requests_cache
import deviantart
from bs4 import BeautifulSoup
import requests
import re, time, random
import pandas as pd
import io
import os, json
import deviantart


class DeviantArtScraper:
    def __init__(self, client_id, client_secret):
        self.da = deviantart.Api(client_id, client_secret)

    def reauthenticate(self):
        """Reauthenticates with the DeviantArt API."""
        try:
            if self.da.is_authorized:
                print("Attempting to refresh token...")
                self.da  # Re-initialize api object
                print("Token refreshed successfully.")
            else:
                print("Authorizing...")
                self.da.authorize()  # Initiate authorization flow if not authorized
                print("Authorization successful.")
        except Exception as e:
            print(f"Reauthentication failed: {e}")
            # Consider re-raising the exception if reauthentication fails:
            # raise

    def scrape(self, username):
        try:
            # --- About Page ---
            about_page_url = f"https://www.deviantart.com/{username}/about"
            about_page_response = requests.get(about_page_url)
            about_page_response.raise_for_status()  # Raise an exception for bad responses

            about_page_soup = BeautifulSoup(about_page_response.content, "html.parser")
            id_pattern = re.compile(r"module-\d{10}")
            # Extract About page (adjust the selector based on the page structure)
            aboutpage = about_page_soup.select_one("div.wuU4s._3tnu8._2yPuh")
            userstats_element = about_page_soup.find(id="userstats")  # Userstats
            # watchers_element = about_page_soup.find(id="watchers")
            # watching_element = about_page_soup.find(id="watching")
            group_element = about_page_soup.find(id="group_list_members")
            badges_element = about_page_soup.find(id="badges_activity")
            about_deviant = about_page_soup.find(id=id_pattern)

            userstats = userstats_element.text.strip() if userstats_element else ""
            # watchers = watchers_element.text.strip() if watchers_element else ""
            # watching = watching_element.text.strip() if watching_element else ""
            groupmem = group_element.text.strip() if group_element else ""
            badges = badges_element.text.strip() if badges_element else ""
            about_devts = about_deviant.text.strip() if about_deviant else ""
            # --- Gallery ---
            gallery_url = f"https://www.deviantart.com/{username}/gallery/"
            gallery_response = requests.get(gallery_url)
            gallery_response.raise_for_status()

            gallery_soup = BeautifulSoup(gallery_response.content, "html.parser")

            # Extract gallery items (adjust the selector based on the page structure)
            gallery_items = gallery_soup.select(".torpedo-thumb-link")  # Example selector
            gallery_links = [item["href"] for item in gallery_items]

            statistics_data = {}

            # Define patterns for each statistic
            patterns_stats = {
                "Pageviews": r"Pageviews(\d+)",
                "Deviations": r"Deviations(\d+)",
                "Watchers": r"Watchers(\d+)",
                "Watching": r"Watching(\d+)",
                "Favourites": r"Favourites(\d+)",
                "Comments Made": r"Comments\sMade(\d+)",  # Handle space in "Comments Made"
                "Comments Received": r"Comments\sReceived(\d+)",  # Handle space in "Comments Received"
            }

            patterns_about_info = {

            }
            # Extract values using patterns
            for key, pattern in patterns.items():
                match = re.search(pattern, userstats)
                if match:
                    statistics_data[key] = int(match.group(1))  # Convert value to integer

            return statistics_data, groupmem, badges, about_devts


        except requests.exceptions.RequestException as e:
            print(f"Error scraping DeviantArt profile: {e}")