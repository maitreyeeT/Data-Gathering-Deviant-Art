import requests
import os
import pandas as pd
from urllib.parse import urlparse
import random
import time
import pandas as pd
import os
import requests
from urllib.parse import urlparse
import random
import time
import shutil  # Import shutil for file operations
import gc


def download_images_from_urls(df_path, artist_column, url_column, output_dir="/mnt/hdd/maittewa/DeviantArt_Deviations/", min_sleep=1, max_sleep=3, chunksize=1000):
    """
    Downloads images from a DataFrame (read in chunks), creating artist-specific directories.
    Skips artists/images that have already been downloaded and significantly improves memory efficiency.

    Args:
        df_path: The path to the CSV file containing the DataFrame.
        artist_column: The name of the column containing the artist names.
        url_column: The name of the column containing the image URLs.
        output_dir: The base directory where artist folders will be created.
        min_sleep: Minimum sleep time in seconds between downloads.
        max_sleep: Maximum sleep time in seconds between downloads.
        chunksize: The number of rows to process at a time.
    """

    # Get a list of already processed artists
    processed_artists = get_processed_artists(output_dir)

    # Use chunksize to read the DataFrame in chunks
    for df_chunk in pd.read_csv(df_path, chunksize=chunksize):
        for artist_name in df_chunk[artist_column].unique():
            if artist_name in processed_artists:
                print(f"Skipping already processed artist: {artist_name}")
                continue

            artist_dir = os.path.join(output_dir, artist_name)

            # Create artist directory if it doesn't exist
            if not os.path.exists(artist_dir):
                os.makedirs(artist_dir)

            artist_df = df_chunk[df_chunk[artist_column] == artist_name]

            # Process each image, using iterrows to fetch the data.
            for index, row in artist_df.iterrows():
                image_url = row[url_column]

                if pd.isna(image_url):
                    print(f"Skipping row {index} for artist {artist_name} due to empty URL.")
                    continue

                # Check if the image has already been downloaded
                filename = os.path.basename(urlparse(image_url).path)
                filepath = os.path.join(artist_dir, filename)
                if os.path.exists(filepath):
                    print(f"Skipping already downloaded image: {image_url}")
                    continue

                try:
                    # Use streaming to handle large files efficiently
                    with requests.get(image_url, stream=True) as response:
                        response.raise_for_status()  # Raise exception for bad status codes

                        # Save the image in chunks
                        with open(filepath, 'wb') as file:
                            shutil.copyfileobj(response.raw, file)

                        print(f"Downloaded: {image_url} to {filepath}")

                except requests.exceptions.RequestException as e:
                    print(f"Error downloading {image_url} for artist {artist_name}: {e}")
                except Exception as e:
                    print(f"An unexpected error occurred with {image_url} for artist {artist_name}: {e}")

                # Sleep for a random duration
                sleep_duration = random.uniform(min_sleep, max_sleep)
                print(f"Sleeping for {sleep_duration:.2f} seconds...")
                time.sleep(sleep_duration)

            # After all images for the artist have been processed, create a flag file
            create_artist_flag_file(output_dir, artist_name)

        # Manual garbage collection
        del df_chunk  # Delete the chunk data from memory
        gc.collect()  # Trigger the garbage collector

def get_processed_artists(output_dir):
    """
    Retrieves a list of artists that have already been processed.

    Args:
        output_dir: The base directory where artist folders are created.

    Returns:
        A set of artist names that have been processed.
    """
    processed_artists = set()
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        return processed_artists

    for item in os.listdir(output_dir):
        item_path = os.path.join(output_dir, item)
        if os.path.isdir(item_path) and os.path.exists(os.path.join(item_path, ".processed")):
            processed_artists.add(item)
    return processed_artists


def create_artist_flag_file(output_dir, artist_name):
    """
    Creates a flag file to mark an artist as processed.

    Args:
        output_dir: The base directory where artist folders are created.
        artist_name: The name of the artist to mark as processed.
    """
    artist_dir = os.path.join(output_dir, artist_name)
    flag_file_path = os.path.join(artist_dir, ".processed")
    with open(flag_file_path, "w") as f:
        f.write("Processed")

'''To run the code on Costello. Provide the location for the dataframe with deviant names (devtn_df) 
and where the deviations (outpt_dir) need to be downloaded.'''
devtn_df = "/mnt/hdd/maittewa/uniqueDev_gall_RndmWalk03_4.csv"
# Download the images (replace 'image_url' with the actual column name)
download_images_from_urls(devtn_df, 'Author_name','Deviation_source')