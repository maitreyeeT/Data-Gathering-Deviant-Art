import pandas as pd, glob, os

# 1. Specify the directory containing your CSV files
csv_directory = '/home/maitreyee/PycharmProjects/DataGatheringDeviantArt/Usernames_merged/'  # Replace with your directory

# 2. Get a list of all CSV files in the directory
csv_files = glob.glob(os.path.join(csv_directory, '*.csv'))

# 3. Create an empty list to store DataFrames
dfs = []

# 4. Iterate through CSV files, read them as DataFrames, and append to the list
for csv_file in csv_files:
    df = pd.read_csv(csv_file)
    dfs.append(df)

# 5. Concatenate all DataFrames into one
merged_df = pd.concat(dfs, ignore_index=True)

# 6. Drop duplicate rows
merged_df.drop_duplicates(subset=["username"], keep='first', inplace=True)


# 7. (Optional) Save the merged DataFrame to a new CSV file
merged_df.to_csv('/home/maitreyee/PycharmProjects/DataGatheringDeviantArt/Usernames_merged/gathered_usernames1.csv', index=False)