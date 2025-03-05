import os
import time
import random
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

# reading in the met txt
f_path = "/home/maittewa/codes/MetObjects.txt"
data = pd.read_csv(f_path)
# for images and parquet file of urls
output_folder = "/mnt/hdd/maittewa/metMuseum"

#getting just id and url
cols = [4, 47]
scrape_dt = data[data.columns[cols]]
scrape_dt["im_link"] = None

# must include the columns: 'Link Resource', 'im_link', and 'Object ID'
os.makedirs(output_folder, exist_ok=True)
big_parquet_file = "codes/complete_data.parquet"

batch_size = 15

# --- Load existing data to identify already scraped rows ---
try:
    scrape_dt = pd.read_parquet(big_parquet_file)
    already_scraped_ids = set(scrape_dt.loc[scrape_dt['im_link'].notna(), 'Object ID'])
    print(f"Loaded existing data with {len(scrape_dt)} rows. {len(already_scraped_ids)} rows were already scraped.")
except FileNotFoundError:
    print("Parquet file not found. Starting fresh.")
    scrape_dt = pd.DataFrame()  # Load or create your initial DataFrame here
    already_scraped_ids = set()

total_rows = len(scrape_dt)

for start in range(0, total_rows, batch_size):  # Start from 0 
    end = min(start + batch_size, total_rows)
    print(f"Processing rows {start} to {end - 1}...")

    # --- Scraping Phase ---
    for index in range(start, end):
        image_id = scrape_dt.loc[index, "Object ID"]

        # --- Skip if already scraped ---
        if image_id in already_scraped_ids:
            print(f"Row {index}: Object ID {image_id} already scraped. Skipping.")
            continue  

        url = scrape_dt.loc[index, "Link Resource"]
        # ... (rest of the scraping and downloading code remains the same) ...

    # --- Save the Entire DataFrame to a Single Parquet File ---
    scrape_dt.to_parquet(big_parquet_file, index=False)
    print(f"Updated the complete Parquet file: {big_parquet_file}")

    # --- Take a Random Snooze Before Next Batch ---
    sleep_time = random.uniform(1, 5)  # Sleep between 1 and 5 seconds
    print(
        f"Batch {start} to {end - 1} complete. Sleeping for {sleep_time:.2f} seconds...\n"
    )
    time.sleep(sleep_time)