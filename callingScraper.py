import pandas as pd
import time
import random
import os
from deviantAbtPgScrape import DeviantArtScraper
# This code doesn't repeat while savings the scraped data of the usernames

# Read usernames from CSV
scraper = DeviantArtScraper("42096", "97080792c6d30a4178965e41f1ca15de")
usernames_df = pd.read_csv("/home/maitreyee/PycharmProjects/DataGatheringDeviantArt/Usernames_merged/gathered_usernames1.csv")  # Replace with your CSV file

# Keep track of already scraped usernames
scraped_usernames = set()
if os.path.exists("/home/maitreyee/PycharmProjects/DataGatheringDeviantArt/deviants60K_2.csv"):
    existing_df = pd.read_csv("/home/maitreyee/PycharmProjects/DataGatheringDeviantArt/deviants60K_2.csv")
    scraped_usernames.update(existing_df['username'].tolist())

# Initialize an empty list to store scraped data
all_about_data = []

batch_size = 10
for i in range(0, len(usernames_df), batch_size):
    batch_usernames = usernames_df['username'][i : i + batch_size].tolist()

    for username in batch_usernames:
        if username not in scraped_usernames:
          try:
            scrape_result = scraper.scrape(username)
                #watchers, watching = scraper.get_watchers_and_watching(username)
            if scrape_result:
                statistics, groupmem, badges, about_info = scrape_result
            else:
                print(f"Skipping {username} due to scraping error")
                continue

            user_data = {
                    "username": username,
                    "statistics": statistics,
                    "about_info": about_info,
                    "group_members_of": groupmem,
                    "Badges": badges
                }
            print(f"Successfully processed user: {username}")  # Print here

            if user_data:
                all_about_data.append(user_data)  # Append to list
                scraped_usernames.add(username)  # Mark as scraped

            time.sleep(random.uniform(5, 15))  # Sleep after each user
          except Exception as e:
                print(f"Error processing user {username}: {e}")
                # Consider logging the error or taking other actions
                time.sleep(random.uniform(5, 15))  # Sleep to avoid potential rate limits


    # Create a DataFrame from the current batch
    batch_df = pd.DataFrame(all_about_data)

    # Save the batch DataFrame to CSV (append if file exists)
    batch_df.to_csv("/home/maitreyee/PycharmProjects/DataGatheringDeviantArt/deviants60K_2.csv", mode="a", header=not os.path.exists("/home/maitreyee/PycharmProjects/DataGatheringDeviantArt/deviants60K_2.csv"), index=False)

    # Clear the list for the next batch
    all_about_data = []

    print(f"Batch {i // batch_size + 1} completed and saved.")

print("Scraping completed.")