import pandas as pd # Import the pandas library for data manipulation
import time # Import the time module for delays
import random # Import the random module for generating random delays
import os # Import the os module for operating system related functionalities, like checking file existence
from deviantAbtPgScrape import DeviantArtScraper # Import a custom class for DeviantArt scraping logic

# This code doesn't repeat while savings the scraped data of the usernames

# --- Configuration and Initialization ---

# Initialize the DeviantArtScraper with client credentials
# These credentials should be obtained from DeviantArt API for authentication.
scraper = DeviantArtScraper("42096", "97080792c6d30a4178965e41f1ca15de")

# Read usernames from a compressed CSV file into a pandas DataFrame.
# The file path is hardcoded. '/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_profileSnwball_fin1_clean.csv.gz' is the input list of DeviantArt users.
usernames_df = pd.read_csv("/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_profileSnwball_fin1_clean.csv.gz")

# Define the path for the output CSV file where scraped data will be stored.
# This file will be created or appended to.
output_csv_path = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviants_snwballScraped_01_testing.csv.gz"

# Initialize an empty set to keep track of usernames that have already been scraped.
# This set is crucial for resuming the process and avoiding re-scraping previously processed users.
scraped_usernames = set()

# Check if the output CSV file already exists and has content.
if os.path.exists(output_csv_path) and os.path.getsize(output_csv_path) > 0:
    try:
        # If the file exists, read its content to identify already scraped usernames.
        existing_df = pd.read_csv(output_csv_path)
        # If the DataFrame is not empty and contains a 'username' column, update the set.
        if not existing_df.empty and 'username' in existing_df.columns:
            scraped_usernames.update(existing_df['username'].tolist())
        print(f"Loaded {len(scraped_usernames)} already scraped usernames from {output_csv_path}.")
    except Exception as e:
        # Handle potential errors during loading, such as corrupted CSV or file permission issues.
        print(f"Error loading existing scraped usernames from {output_csv_path}: {e}. Starting fresh for visited check.")

# Initialize an empty list to temporarily store scraped data for the current batch.
# This list will be converted to a DataFrame and saved after each batch.
all_about_data = []

# Define the number of usernames to process in each batch before saving to CSV.
batch_size = 10

# --- Main Scraping Loop ---

# Iterate through the DataFrame of usernames, processing them in chunks (batches).
for i in range(0, len(usernames_df), batch_size):
    # Extract a slice of usernames for the current batch.
    batch_usernames = usernames_df['username'][i : i + batch_size].tolist()

    # Process each username within the current batch.
    for username in batch_usernames:
        # Check if the current username has already been processed.
        if username not in scraped_usernames:
            try:
                # Attempt to scrape the user's profile data using the DeviantArtScraper instance.
                # The 'scrape' method is expected to return a tuple of (statistics_data, groupmem, badges, about_devts).
                scrape_result = scraper.scrape(username)

                # If scraping was successful (i.e., scrape_result is not None and valid).
                if scrape_result:
                    # Unpack the results into individual variables.
                    statistics, groupmem, badges, about_info = scrape_result

                    # Create a dictionary to store the extracted data for the current user.
                    user_data = {
                        "username": username,
                        "statistics": statistics,
                        "about_info": about_info,
                        "group_members_of": groupmem,
                        "Badges": badges
                    }
                    
                    # Append the user's data dictionary to the list for the current batch.
                    all_about_data.append(user_data)
                    # Add the username to the set of scraped users to mark it as processed.
                    scraped_usernames.add(username)
                    print(f"Successfully processed user: {username}")
                else:
                    # If scrape_result is None or empty, it indicates an error during scraping for this user.
                    print(f"Skipping {username} due to scraping error or no data returned.")

                # Introduce a random delay between 5 and 15 seconds.
                # This helps to avoid overwhelming the DeviantArt servers and respects API/scraping rate limits.
                time.sleep(random.uniform(5, 15))

            except Exception as e:
                # Catch any unexpected errors that occur during the processing of a single user.
                print(f"Error processing user {username}: {e}")
                # Introduce a delay even on error to prevent rapid-fire retries that could exacerbate issues.
                time.sleep(random.uniform(5, 15))

    # --- Batch Saving ---

    # Create a pandas DataFrame from the data collected in the current batch.
    batch_df = pd.DataFrame(all_about_data)

    # Save the batch DataFrame to the output CSV file.
    # mode="a" appends data if the file exists, otherwise it creates a new file.
    # header=not os.path.exists(output_csv_path) ensures the header is written only once (for the first batch) and not for subsequent appends.
    batch_df.to_csv(output_csv_path, mode="a", header=not os.path.exists(output_csv_path), index=False)

    # Clear the list that stores data for the current batch to prepare for the next batch.
    all_about_data = []

    # Print a message indicating the completion of the current batch.
    print(f"Batch {i // batch_size + 1} completed and saved.")

# --- Finalization ---

# Print a final message once all usernames have been processed and saved.
print("Scraping completed.")
