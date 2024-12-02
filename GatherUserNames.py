import deviantart, requests, time, random, requests_cache
import pandas as pd
import glob, os
# Replace with your client ID and client secret
client_id = "42096"
client_secret = "97080792c6d30a4178965e41f1ca15de"

da = deviantart.Api(client_id, client_secret)
#requests_cache.install_cache('deviantart_cache', backend='sqlite', expire_after=3600)

def reauthenticate():
    """Reauthenticates with the DeviantArt API."""
    global da
    try:
        if da.access_token:
            print("Attempting to refresh token...")
            da.refresh_token()  # Re-initialize api object
            print("Token refreshed successfully.")
        else:
            print("Authorizing...")
            da.auth()  # Initiate authorization flow if not authorized
            print("Authorization successful.")
    except Exception as e:
        print(f"Reauthentication failed: {e}")
        # Consider re-raising the exception if reauthentication fails:
        # raise

def get_deviants_by_tag(tag, limit=200, initial_cursor = None):
    """Gathers unique deviants contributing to a specific tag using the DeviantArt API."""
    deviants = [] # Use a list to store usernames and tags
    offset = 0
    has_more = True
    cursor = initial_cursor

    while has_more:
        try:
            results = da.browse(endpoint="tags", tag=tag, offset=offset, limit=limit)

            if 'results' in results and isinstance(results['results'], list):
                for deviation in results['results']:
                    username = deviation.author.username
                    deviants.append({'username': username, 'tag': tag})  # Store username and tag

                has_more = 'next_offset' in results and results['next_offset'] is not None
                if has_more:
                    offset = results['next_offset']
                    time.sleep(random.uniform(1, 3))  # Add a delay

            else:
                print(f"Unexpected API response format for tag '{tag}'.")
                has_more = False

        except requests.exceptions.HTTPError as e:
            print(f"Entering HTTPError handler...")
            print(f"HTTP Error: {e} for tag: {tag}")
            if e.response.status_code == 401:  # Unauthorized
                print(f"Re authenticating for tag '{tag}'...")
                #reauthenticate()  # Assuming you have a reauthenticate function
                deviantart.Api(client_id, client_secret)
            #status_code_match = re.search(r"(\d{3})", str(e))  # Extract status code from error message
            #if status_code_match and status_code_match.group(1) == "401":
            #    print("401 Unauthorized error. Reauthenticating...")
            #    reauthenticate()
            else:
                # Handle other HTTP errors
                print(f"Handling other HTTP errors...")
                break

        except Exception as e:
            print(f"Entering general exception handler...")
            print(f"Error: {e} for tag: {tag}")
            has_more = False

    return deviants  # Return the list of usernames and tags
def process_tags_from_csv(csv_file, start_index, end_index, batch_size=20):
    """Reads tags, gets usernames and tags, saves to CSV, and downloads after each loop."""
    output_file = "deviants_nonmatchTags885-924.csv"
    df_tags = pd.read_csv(csv_file)
    all_deviants = []  # To store all usernames and tags
    count = 0
    # Process tags within the specified range
    for i in range(start_index, min(end_index, len(df_tags)), batch_size):
      tags_batch = df_tags['tag_name'][i:i + batch_size].tolist()
      for tag_with_cursor in tags_batch:
        # Split tag and cursor (if cursor is present)
        initial_cursor = None
        if "?" in tag_with_cursor:
          tag, cursor_part = tag_with_cursor.split("?", 1)
          cursor_params = dict(p.split("=") for p in cursor_part.split("&"))
          iniital_cursor = cursor_params.get("cursor")
        else:
          tag = tag_with_cursor

        deviants = get_deviants_by_tag(tag, initial_cursor)

        print(f"Processed tag: {tag}")
        all_deviants.extend(deviants)
        time.sleep(random.uniform(2, 5))
        count += 1

        # Dump to JSON after every 10 usernames
        if count % 10 == 0:
            df_deviants = pd.DataFrame(all_deviants)
            df_deviants.drop_duplicates(subset=['username'], inplace=True)
            df_deviants.to_csv(output_file, mode='a', header=not os.path.exists(output_file), index=False, encoding='utf-8')
            all_data = []  # Clear data list for the next batch
            print(f"Dumped data for {count} usernames to {output_file}")

    # Create DataFrame and remove duplicates

    # Save to CSV
    print(f"Unique usernames and tags saved.")
#Next do 137-238
# Example usage:
input_csv_file = "non_matching_rows.csv"
process_tags_from_csv(input_csv_file, start_index=885, end_index=924)