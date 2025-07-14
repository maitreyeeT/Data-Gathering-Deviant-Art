import requests
from bs4 import BeautifulSoup
import re
import random
import time
import pandas as pd
import os
import csv # Import the csv module for quoting constants

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException



# Define the Selenium Scraper class
class DeviantArtScraperWithSelenium:
    def __init__(self, driver_path=None):
        """
        Initializes the scraper with Selenium.

        Args:
            driver_path: Path to the WebDriver executable (e.g., 'chromedriver').
                         If None, assumes the driver is in your system's PATH.
        """
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        # Add a user-agent to make requests appear more like a regular browser
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        options.add_argument(f'user-agent={user_agent}')


        self.driver = None # Initialize driver to None
        try:
            if driver_path:
                self.driver = webdriver.Chrome(executable_path=driver_path, options=options)
            else:
                # Ensure chromedriver is in your system's PATH or provide the path
                self.driver = webdriver.Chrome(options=options)
        except WebDriverException as e:
            print(f"Error initializing WebDriver. Make sure chromedriver is correctly installed and in your PATH or provide driver_path. Error: {e}")
            # The driver remains None if initialization fails


    def __del__(self):
        """Ensures the browser is closed when the object is deleted."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                print(f"Error closing WebDriver: {e}")


    def load_more_content(self, selector, timeout=10):
        """
        Clicks a "Load more" button repeatedly until it's no longer visible or clickable.

        Args:
            selector: The CSS selector for the "Load more" button.
            timeout: The maximum time to wait for the button to be clickable.
        """
        if not self.driver:
            print("WebDriver not initialized. Cannot load more content.")
            return

        while True:
            try:
                load_more_button = WebDriverWait(self.driver, timeout).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                )
                print(f"Clicking '{selector}' button...")
                # Use JavaScript to click the button - sometimes more reliable
                self.driver.execute_script("arguments[0].click();", load_more_button)
                time.sleep(random.uniform(2, 5)) # **Increased delay after clicking**

            except (TimeoutException, NoSuchElementException):
                # Button is no longer found or clickable, exit the loop
                print(f"'{selector}' button not found or no more content to load.")
                break
            except Exception as e:
                print(f"An error occurred while clicking '{selector}': {e}")
                break # Exit loop on unexpected error


    def scrape_profile_sections(self, username):
        """
        Scrapes the "About", "Watching", "Group Memberships", and "Badges" sections,
        handling "Load more" buttons using Selenium.

        Args:
            username: The DeviantArt username.

        Returns:
            A dictionary containing the scraped data, or None if scraping fails.
        """
        if not self.driver:
            print(f"Skipping scraping for {username}: WebDriver not initialized.")
            return None

        url = f"https://www.deviantart.com/{username}/about"

        try:
            self.driver.get(url)

            # Add a wait for a general element that should be present on the page
            WebDriverWait(self.driver, 30).until( # **Increased timeout**
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(random.uniform(3, 6)) # **Increased delay after initial load**


            # --- Handle "Load more" buttons for each section ---

            # Use the confirmed selector for the Watching section
            watching_load_more_selector = "#watching > button"

            # You'll need to find the selectors for Group Memberships and Badges buttons as well
            # IMPORTANT: Replace these with the actual selectors you find
            group_load_more_selector = "#group_list_members > button"
            #badges_load_more_selector = "#badges_activity button.load-more-button-selector"


            print("Loading all 'Watching' content...")
            self.load_more_content(watching_load_more_selector)

            # Load content for other sections if they have load more buttons
            print("Loading all 'Group Membership' content...")
            self.load_more_content(group_load_more_selector)

            # print("Loading all 'Badges' content...")
            # self.load_more_content(badges_load_more_selector)


            # --- Extract the fully loaded content (including About) ---

            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')

            # --- Extract the "About" information using the stable pattern ---
            about_info_text = ""
            # Using the stable selector based on ID pattern and consistent class names
            about_section_element = soup.select_one('section[id^="module-"]._3EJ9T._3OvU8._27q1_')

            if about_section_element:
                about_info_text = about_section_element.get_text(separator=' ', strip=True)
                # Consider further cleaning of about_info_text if needed


            # Extract other elements using BeautifulSoup from the full source
            watching_element = soup.find(id="watching")
            group_element = soup.find(id="group_list_members")
            badges_element = soup.find(id="badges_activity")

            # Extract text content - use get_text for comprehensive extraction
            watching_text = watching_element.get_text(separator=' ', strip=True) if watching_element else ""
            groupmem_text = group_element.get_text(separator=' ', strip=True) if group_element else ""
            badges_text = badges_element.get_text(separator=' ', strip=True) if badges_element else ""

            # You'll need to figure out how to extract "statistics" if that's still needed
            # Inspect the page to find where statistics like pageviews, comments, etc. are located
            # and add code here to find and extract them using soup.find() or soup.select_one()
            statistics_data = {} # Placeholder for statistics

            return {
                "username": username,
                "about_info": about_info_text,
                "group_members_of": groupmem_text,
                "Badges": badges_text,
                "Watching": watching_text
            }

        except Exception as e:
            print(f"An error occurred while scraping {username}: {e}")
            return None # Return None on error

# --- Main Execution Block ---

if __name__ == "__main__":
    # Initialize the Selenium scraper
    # Replace with the path to your chromedriver executable if it's not in PATH
    # driver_path = "/path/to/your/chromedriver"
    # scraper = DeviantArtScraperWithSelenium(driver_path=driver_path)

    scraper = DeviantArtScraperWithSelenium() # Assumes chromedriver is in PATH

    if not scraper.driver:
         print("Scraper initialization failed. Exiting.")
    else:
        # Read usernames from CSV
        usernames_df = pd.read_csv("/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_profileSnwball_fin1_clean.csv.gz") #deviant column is user

       # Keep track of already scraped usernames
        scraped_usernames = set()
        output_file = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviants_snwballScraped_fin01.csv.gz" #deviant column is username
        if os.path.exists(output_file):
            try:
                existing_df = pd.read_csv(output_file)
                if 'username' in existing_df.columns: # Assuming the 'user' column exists in your output CSV
                    scraped_usernames.update(existing_df['username'].tolist())
                else:
                    print(f"Warning: 'username' column not found in '{output_file}'. Cannot track scraped users.")
            except Exception as e:
                print(f"Error reading existing scraped data from '{output_file}': {e}")
        
        # Filter usernames_df to only include usernames not in scraped_usernames
        usernames_to_scrape_df = usernames_df[~usernames_df['user'].isin(scraped_usernames)].reset_index(drop=True)
        
        # Print the number of skipped usernames
        num_skipped = len(usernames_df) - len(usernames_to_scrape_df)
        print(f"Skipping {num_skipped} usernames that have already been scraped.")
        
        # Initialize an empty list to store scraped data
        all_about_data = []
        
        batch_size = 1
        for i in range(0, len(usernames_to_scrape_df), batch_size):
            batch_usernames = usernames_to_scrape_df['user'][i : i + batch_size].tolist()
        
            for username in batch_usernames:
                try:
                    print(f"Scraping the data for {username}")
                    scrape_result = scraper.scrape_profile_sections(username)
                    print(f"Scraped the data for {username}")
                    if scrape_result:
                        # Append the dictionary directly to the list
                        all_about_data.append(scrape_result)
                        scraped_usernames.add(username) # Add to the set of scraped usernames
                    else:
                        print(f"Failed to scrape data for user: {username}")
    
                    # Add a delay between processing each user
                    time.sleep(random.uniform(5, 15)) # **Delay after each user**
                except Exception as e:
                    print(f"Error processing user {username}: {e}")
                    time.sleep(random.uniform(5, 15))  # Sleep to avoid potential rate limits
        
                if all_about_data: # Only save if there's data in the current batch
                    batch_df = pd.DataFrame(all_about_data)
                            
                            # *** CRITICAL FIX HERE ***
                    batch_df.to_csv(
                                output_file,
                                mode="a",
                                header=not os.path.exists(output_file) if len(scraped_usernames) == 0 else False, # Only write header if file is new AND no users were loaded from it
                                index=False,
                                # Add quoting and escapechar for robustness against special characters
                                quoting=csv.QUOTE_MINIMAL, # Enclose fields with special characters in quotes
                                escapechar='\\',           # Use backslash as escape character for problematic characters within quoted fields
                                encoding='utf-8'           # Ensure proper handling of various characters
                            )
                    print(f"Batch {i // batch_size + 1} completed and saved to {output_file}.")
                    all_about_data = [] # Clear the list for the next batch
                else:
                    print(f"Batch {i // batch_size + 1} completed but no data to save.")
                        
                    time.sleep(5) # Small delay between batches
                        
        print("\n--- Scraping Completed ---")

    # The scraper.driver.quit() is handled by the __del__ method when the scraper object goes out of scope.