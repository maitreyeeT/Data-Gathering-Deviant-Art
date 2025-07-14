import pandas as pd
import re # Import regex module
import ast # Keep ast for potential fallback or other parsing needs, though not used directly for splitting here

import numpy as np
from sqlalchemy import UniqueConstraint, inspect, create_engine, text,  Column, Integer, String, ForeignKey, Boolean, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import os
import ast  # To safely evaluate string representations of lists

# Define the Base if it's not already defined in your notebook


# Define the Artist class if it's not already defined (assuming it exists)
Base = declarative_base()
DATABASE_URL =  'sqlite:////mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_main05.db'
engine = create_engine(DATABASE_URL)  # Create a SQLite database file
Session = sessionmaker(bind=engine)

class Artist(Base):
    __tablename__ = 'artists'
    id = Column(Integer, primary_key=True)
    artist_name = Column(String)
    profile_url = Column(String)
    country = Column(String)
    level = Column(String)
    registration_date = Column(Integer)
    no_of_deviations = Column(Integer)  
    no_of_favourites = Column(Integer)
    no_of_user_comments = Column(Integer)
    no_of_pageviews = Column(Integer)
    no_of_profile_comments = Column(Integer)
    is_artist  = Column(Boolean)
    gender = Column(String)
    speciality = Column(String)
    no_of_images = Column(Integer)
    no_of_AI_images = Column(Integer)
    ai_adopter = Column(Boolean)
    ai_adoption_first_time = Column(Integer) 
    #Relationship description
    watchings = relationship("Watching", back_populates="artist") # Establish relationship from Artist to watching

# 3. Define Watching table
class Watching(Base):
    __tablename__ = 'watchings'
    id = Column(Integer, primary_key=True)
    artist_id = Column(Integer, ForeignKey('artists.id')) # Foreign key to Artist table
    watching_name = Column(String)
    artist = relationship("Artist", back_populates="watchings")

# Add relationships to Artist class (if not already present)
# These relationships should be added to the existing Artist class definition

Artist.watchings = relationship("Watching", back_populates="artist")

# Create tables (if they don't exist)
# This will create only the new tables defined above if Base.metadata.create_all(engine) was already called
Base.metadata.create_all(engine)

def process_watching_data(csv_path):
    """
    Reads scraping CSV, extracts watching relationships, and returns a list of dictionaries.

    Args:
        csv_path: Path to the scraping CSV file.

    Returns:
        A list of dictionaries, where each dictionary represents a single
        watching relationship: [{'username': 'ArtistA', 'watching_name': 'ArtistB'}, ...]
    """
    processed_watchings = []
    try:
        # Adjust chunksize as needed
        for chunk in pd.read_csv(csv_path, chunksize=1000, on_bad_lines='skip'):
            for index, row in chunk.iterrows():
                artist_name_who_is_watching = row['username'] # Column with the watching artist's username

                # Check if 'Watching' column is not null or empty
                if pd.notnull(row['Watching']) and str(row['Watching']).strip():
                    watching_string = str(row['Watching']).strip()

                    # Use regex to remove "Watching XXX Deviants" at the beginning
                    # This regex looks for "Watching", followed by one or more spaces,
                    # followed by one or more digits (\d+), followed by one or more spaces,
                    # followed by "Deviants" (case-insensitive), followed by optional spaces.
                    # Use regex to remove "Watching XXX Deviants" at the beginning
                    watching_string_cleaned = re.sub(r'^Watching\s+\d+\s+Deviants\s*', '', watching_string, flags=re.IGNORECASE)

                    watched_usernames = watching_string_cleaned.split()

                    for watching_name in watched_usernames: # This is where the error occurs
                        # Check if the watching_name is not empty after splitting
                        if watching_name.strip():
                            processed_watchings.append({
                                'username': artist_name_who_is_watching,
                                'watching_name': watching_name.strip()
                            })
                # Handle the case where 'Watching' column is null or empty after stripping
                elif pd.isnull(row['Watching']) or not str(row['Watching']).strip():
                    # You might want to log this or take other action if needed
                    print(f"Info: 'Watching' column is empty or null for artist {artist_name_who_is_watching}. Skipping.")


    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_path}")
    except Exception as e:
        print(f"An error occurred during watchings data processing: {e}")

    return processed_watchings


def load_watchings_incrementally(session, processed_data):
#     """
#     Loads processed watching data incrementally into the Watching table.
#
#     Args:
#         session: SQLAlchemy session object.
#         processed_data: A list of dictionaries from process_watching_data.
#     """
     total_loaded = 0
     try:
         chunk_size = 1000
         for i in range(0, len(processed_data), chunk_size):
             chunk = processed_data[i : i + chunk_size]
             for row_data in chunk:
                 artist_name_who_is_watching = row_data['username']
                 watching_name = row_data['watching_name']

                 artist_who_is_watching = session.query(Artist).filter_by(artist_name=artist_name_who_is_watching).first()
#
                 if artist_who_is_watching:
                     watched_artist = session.query(Artist).filter_by(artist_name=watching_name).first()
                     watching_artist_id = watched_artist.id if watched_artist else None
#
                     existing_record = session.query(Watching).filter_by(
                         artist_id=artist_who_is_watching.id,
                     ).first()

                     if not existing_record and watching_artist_id is None:
                          existing_record = session.query(Watching).filter_by(
                             artist_id=artist_who_is_watching.id,
                             watching_name=watching_name,
                          ).first()

                     if not existing_record:
                         new_watching = Watching(
                             artist_id=artist_who_is_watching.id,
                             watching_name=watching_name,
                         )
                         session.add(new_watching)
                         total_loaded += 1
                 else:
                     print(f"Warning: Artist '{artist_name_who_is_watching}' from processed data not found in the database. Skipping.")
#
             session.commit()
             print(f"Loaded a chunk of processed watchings. Total loaded: {total_loaded}")
#
     except Exception as e:
         session.rollback()
         print(f"An error occurred during incremental watchings data saving: {e}")


# --- Main Execution ---
if __name__ == "__main__":
    session = Session()

    # CSV file paths
    scraping_csv = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviants_snwballScraped_fin01.csv.gz" # Your scraping CSV with 'Watching' column

    # Step 1: Process the watching data and print (optional for verification)
    print("Processing watching data from CSV...")     
    processed_watching_list = process_watching_data(scraping_csv)
     # You can print a sample of processed_watching_list here to verify
    print("Sample of processed data:", processed_watching_list[:10])

    
    # Load data into the new table
    print("\nLoading watchings data...")
    load_watchings_incrementally(session, processed_watching_list)


    session.close()
    print("\nDatabase loading complete.")