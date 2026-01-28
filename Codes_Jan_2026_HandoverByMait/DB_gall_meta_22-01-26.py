import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, BigInteger, Boolean, func, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import re, ast
import numpy as np
from sqlalchemy import UniqueConstraint, inspect, create_engine, text
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import threading # Import the threading module
import threading
import logging
from dateutil import parser # Import parser for date conversion
import pytz # Import pytz for timezone handling


# Assuming Artist, Gallery, and MetaData models are defined in previous cells
# Assuming engine and Session are also defined in previous cells

DATABASE_URL =  'sqlite:////mnt/hdd/maittewa/deviantArt_DeviantData/dbData/deviantArt_main05.db'
engine = create_engine(DATABASE_URL)  # Create a SQLite database file
Base = declarative_base()
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
    gallery = relationship("Gallery", back_populates="artist")
    metaData = relationship("MetaData", back_populates="artist_rel") # Corrected back_populates name to match MetaData attribute
    __table_args__ = (UniqueConstraint('artist_name', name='_artist_name_uc'),) # Added unique constraint

class Gallery(Base):
    __tablename__ = 'gallery'
    id = Column(String, primary_key=True)
    Deviation_url = Column(String)
    Deviation_title = Column(String)
    artist_id = Column(Integer, ForeignKey('artists.id'))  # Link to Artist table
    artist_name = Column(String) # Added artist_name column
    artist_id_onPlatform = Column(String) # Added artist_id_onPlatform column
    Author_type = Column(String)
    Published_on = Column(BigInteger)
    Deviation_source = Column(String)
    Deviation_height = Column(Integer)
    Deviation_width  = Column(Integer)
    Deviation_transparency = Column(Boolean)
    Comments  = Column(Integer)
    is_Mature = Column(Boolean)
    is_Downloadable = Column(Boolean)
    Favourites = Column(Integer)
    artist = relationship("Artist", back_populates="gallery") # Corrected back_populates name
    __table_args__ = (UniqueConstraint('id', name='_gallery_id_uc'),) # Added unique constraint on id

class MetaData(Base):
    __tablename__ = 'meta' # Corrected table name to 'meta' as used in the load function
    id = Column(String, primary_key=True)
    Devtn_Title = Column(String)  # Link to Gallery table
    Devtn_Descp = Column(String)
    artist_id = Column(Integer, ForeignKey('artists.id'))  # Link to Artist table
    artist_name = Column(String) # Added artist_name column
    artist_id_onPlatform = Column(String) # Added artist_id_onPlatform column
    Author_Icon = Column(String)
    Allows_Comments  = Column(Boolean)
    Is_Favourited  = Column(Boolean)
    Can_post_comments  = Column(Boolean)
    Tags_Info = Column(String)
    tag_name = Column(String)
    Sponsered = Column(Boolean)
    Sponser = Column(Boolean)
    artist_rel = relationship("Artist", back_populates="metaData") # Renamed relationship to artist_rel and corrected back_populates name
    __table_args__ = (UniqueConstraint('id', name='_meta_id_uc'),) # Added unique constraint on id

Base.metadata.create_all(engine)

# --- Converters for pandas.read_csv (unchanged) ---
def bool_converter(x):
    if pd.isna(x) or not isinstance(x, str):
        return None
    return str(x).lower() == 'true'

def iso_date_to_ms(x):
    if pd.isna(x) or x is None or str(x).strip() == '':
        return None

    # Try to convert to int first, assuming it's a Unix timestamp (seconds)
    try:
        # Check if it's a string representation of an integer or an actual integer
        if isinstance(x, (int, float)):
            # If it's a float, ensure it's not a NaN and can be safely converted to int
            if isinstance(x, float) and (pd.isna(x) or not x.is_integer()):
                raise ValueError("Not a valid integer timestamp")
            return int(x * 1000) # Convert seconds to milliseconds
        elif isinstance(x, str):
            # Handle strings that might be integer timestamps
            return int(float(x) * 1000) # Convert seconds to milliseconds, handling potential float strings
    except (ValueError, TypeError):
        # If it's not a direct integer or convertible string integer, try parsing as a date string
        pass # Fall through to date string parsing

    try:
        dt_obj = parser.parse(str(x)) # Ensure x is string for parser
        if dt_obj.tzinfo is None:
             dt_obj = pytz.utc.localize(dt_obj)
        else:
            dt_obj = dt_obj.astimezone(pytz.utc)
        return int(dt_obj.timestamp() * 1000)
    except Exception as e:
        logger.warning(f"Could not convert date string '{x}' to timestamp. Error: {e}. Setting to None.")
        return None

# --- Shared Dtype for CSV Reading ---
COMMON_COL_DTYPES = {
    'Author_Icon': str, 'Devtn_Descp': str, 'artist_name': str, 'tag_name': str, 'Deviation_source': str,
    'Deviation_title ': str, 'Tags_Info': str, 'Author_Type': str, 'License': str, 'Author_name': str, 'Author_id': str, 'Devtn_Id': str # Added keys based on usage
}

COMMON_CONVERTERS = {
    'Sponser': bool_converter, 'Sponsered': bool_converter, 'Can_post_comments': bool_converter,
    'Is_Mature': bool_converter, 'Is_Favourited': bool_converter, 'Allows_Comments': bool_converter,
    'is_Downloadable': bool_converter, 'Deviation_transparency': bool_converter, 'Published_on': iso_date_to_ms
}


# --- Helper to get existing artist IDs once ---
def get_artist_name_to_id_map(session):
    logger.info("Fetching all existing artist names and IDs...")
    artist_map = {}
    for artist_id, artist_name in session.query(Artist.id, Artist.artist_name).all():
        artist_map[str(artist_name)] = artist_id
    logger.info(f"Loaded {len(artist_map)} existing artists.")
    return artist_map



# Configure Logging (optional but recommended for tracking)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Assuming Artist, Gallery, and MetaData models are defined in previous cells
# Assuming engine and Session are also defined in previous cells

# --- Data Loading Functions ---

def load_gallery_data(engine, csv_path, chunksize=10000):
    """Loads gallery data from CSV into the database incrementally."""
    Session = sessionmaker(bind=engine)
    session = Session()
    total_rows_processed = 0
    total_rows_skipped_existing = 0
    total_rows_skipped_no_artist = 0 # Counter for skipped rows due to no artist match

    logger.info(f"Starting incremental CSV load for Gallery from {csv_path}")

    try:
        # Use dtype and converters for more robust reading
        for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunksize, on_bad_lines='skip', dtype=COMMON_COL_DTYPES, converters=COMMON_CONVERTERS)):
            logger.info(f"Processing chunk {i + 1} for Gallery (rows {total_rows_processed + total_rows_skipped_existing + total_rows_skipped_no_artist + 1} to {total_rows_processed + total_rows_skipped_existing + total_rows_skipped_no_artist + len(chunk)})")

            # Create a list of Gallery objects to insert
            gallery_to_add = []
            for index, row in chunk.iterrows():
                # Convert relevant columns to appropriate types, handling potential errors

                deviation_id = row.get('Deviation_id')
                artist_id_onPlatform = row.get('Author_id') # Assuming Author_id from CSV maps to artist_id_onPlatform
                artist_name_from_csv = row.get('Author_name')


                # Skip if essential data is missing
                if not deviation_id or pd.isna(artist_name_from_csv) : # Check for NaN in artist_name
                    logger.warning(f"Skipping row {total_rows_processed + total_rows_skipped_existing + total_rows_skipped_no_artist + index + 1} due to missing essential data (Deviation_id or Author_name).")
                    continue

                # Find the artist's integer ID from the Artist table
                # This requires the Artist table to be populated first.
                artist = session.query(Artist).filter_by(artist_name=artist_name_from_csv).first()
                if not artist:
                    logger.warning(f"Skipping row {total_rows_processed + total_rows_skipped_existing + total_rows_skipped_no_artist + index + 1} for Deviation_id {deviation_id}: No matching artist found with name '{artist_name_from_csv}' in the Artist table.")
                    total_rows_skipped_no_artist += 1
                    continue # Skip if no matching artist

                artist_id = artist.id
                artist_name = artist.artist_name # Use the name from the Artist table to ensure consistency


                # Check if record already exists using Deviation_id (primary key)
                existing_record = session.query(Gallery).filter_by(id=deviation_id).first()
                if existing_record:
                    total_rows_skipped_existing += 1
                    continue # Skip if record with this Deviation_id already exists

                try:
                    gallery_entry = Gallery(
                        id=str(deviation_id), # Ensure ID is string
                        Deviation_url=row.get('Deviation_url'),
                        Deviation_title=row.get('Deviation_title'),
                        artist_id=artist_id, # Use the integer artist_id
                        artist_name=artist_name, # Use the artist_name from the Artist table
                        artist_id_onPlatform=str(artist_id_onPlatform) if not pd.isna(artist_id_onPlatform) else None, # Ensure string type, handle NaN
                        Author_type=row.get('Author_type'),
                        Published_on=row.get('Published_on'),
                        Deviation_source=row.get('Deviation_source'),
                        Deviation_height=row.get('Deviation_height'),
                        Deviation_width=row.get('Deviation_width'),
                        Deviation_transparency=row.get('Deviation_transparency'),
                        Comments=row.get('Comments'),
                        is_Mature=row.get('is_Mature'),
                        is_Downloadable=row.get('is_Downloadable'),
                        Favourites=row.get('Favourites')
                    )
                    gallery_to_add.append(gallery_entry)
                except Exception as e:
                    logger.error(f"Error creating Gallery object for row {total_rows_processed + total_rows_skipped_existing + total_rows_skipped_no_artist + index + 1} (Deviation ID: {deviation_id}): {e}")


            # Bulk insert the collected objects
            if gallery_to_add:
                try:
                    session.bulk_save_objects(gallery_to_add)
                    session.commit()
                    total_rows_processed += len(gallery_to_add)
                    logger.info(f"Successfully inserted {len(gallery_to_add)} Gallery records from chunk {i + 1}.")
                except SQLAlchemyError as e:
                    session.rollback()
                    logger.error(f"Error during bulk insert for Gallery chunk {i + 1}: {e}")

    except Exception as e:
        logger.error(f"An error occurred during Gallery data loading: {e}", exc_info=True)
    finally:
        session.close()
        logger.info(f"Finished Gallery data load. Total processed: {total_rows_processed}. Total skipped (existing): {total_rows_skipped_existing}. Total skipped (no artist match): {total_rows_skipped_no_artist}")


def load_metadata(engine, csv_path, chunksize=10000):
    """Loads metadata from CSV into the database incrementally."""
    Session = sessionmaker(bind=engine)
    session = Session()
    total_rows_processed = 0
    total_rows_skipped_existing = 0
    total_rows_skipped_no_artist = 0 # Counter for skipped rows due to no artist match


    logger.info(f"Starting incremental CSV load for MetaData from {csv_path}")

    try:
        # Use dtype and converters for more robust reading
        for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunksize, on_bad_lines='skip', dtype=COMMON_COL_DTYPES, converters=COMMON_CONVERTERS)):
            logger.info(f"Processing chunk {i + 1} for MetaData (rows {total_rows_processed + total_rows_skipped_existing + total_rows_skipped_no_artist + 1} to {total_rows_processed + total_rows_skipped_existing + total_rows_skipped_no_artist + len(chunk)})")

            # Create a list of MetaData objects to insert
            metadata_to_add = []
            for index, row in chunk.iterrows():
                 # Convert relevant columns to appropriate types
                deviation_id = row.get('Devtn_Id') # Assuming this maps to the 'id' column in MetaData table
                artist_name_from_csv = row.get('Author_Name') # Corrected key to 'Author_Name'


                # Skip if essential data is missing
                if not deviation_id or pd.isna(artist_name_from_csv): # Check for NaN in artist_name
                    logger.warning(f"Skipping row {total_rows_processed + total_rows_skipped_existing + total_rows_skipped_no_artist + index + 1} due to missing essential data (Devtn_Id or Author_Name).")
                    continue

                 # Find the artist's integer ID from the Artist table
                 # This requires the Artist table to be populated first.
                artist = session.query(Artist).filter_by(artist_name=artist_name_from_csv).first()
                if not artist:
                    logger.warning(f"Skipping row {total_rows_processed + total_rows_skipped_existing + total_rows_skipped_no_artist + index + 1} for Deviation_id {deviation_id}: No matching artist found with name '{artist_name_from_csv}' in the Artist table.")
                    total_rows_skipped_no_artist += 1
                    continue # Skip if no matching artist

                artist_id = artist.id
                artist_name = artist.artist_name # Use the name from the Artist table to ensure consistency


                # Check if record already exists using 'id' (primary key)
                existing_record = session.query(MetaData).filter_by(id=deviation_id).first()
                if existing_record:
                    total_rows_skipped_existing += 1
                    continue # Skip if record with this id already exists

                try:
                    metadata_entry = MetaData(
                        id=str(deviation_id), # Ensure ID is string
                        Devtn_Title=row.get('Devtn_Title'),
                        Devtn_Descp=row.get('Devtn_Descp'),
                        artist_id= artist_id, # Use the integer artist_id
                        artist_name=artist_name, # Use the artist_name from the Artist table
                        artist_id_onPlatform = str(row.get('Author_Id')) if not pd.isna(row.get('Author_Id')) else None, # Added mapping for artist_id_onPlatform
                        Author_Icon=row.get('Author_Icon'),
                        Allows_Comments=row.get('Allows_Comments'),
                        Is_Favourited=row.get('Is_Favourited'),
                        Can_post_comments=row.get('Can_post_comments'),
                        Tags_Info=row.get('Tags_Info'), # Store as string; parsing into imgs_tags table happens separately
                        tag_name=row.get('tag_name'), # Store as string/list-string as is from CSV
                        Sponsered=row.get('Sponsered'),
                        Sponser=row.get('Sponser')
                    )
                    metadata_to_add.append(metadata_entry)
                except Exception as e:
                    logger.error(f"Error creating MetaData object for row {total_rows_processed + total_rows_skipped_existing + total_rows_skipped_no_artist + index + 1} (Deviation ID: {deviation_id}): {e}")


            # Bulk insert the collected objects
            if metadata_to_add:
                try:
                    session.bulk_save_objects(metadata_to_add)
                    session.commit()
                    total_rows_processed += len(metadata_to_add)
                    logger.info(f"Successfully inserted {len(metadata_to_add)} MetaData records from chunk {i + 1}.")
                except SQLAlchemyError as e:
                    session.rollback()
                    logger.error(f"Error during bulk insert for MetaData chunk {i + 1}: {e}")

    except Exception as e:
        logger.error(f"An error occurred during MetaData data loading: {e}", exc_info=True)
    finally:
        session.close()
        logger.info(f"Finished MetaData data load. Total processed: {total_rows_processed}. Total skipped (existing): {total_rows_skipped_existing}. Total skipped (no artist match): {total_rows_skipped_no_artist}")


# --- Main Execution ---
if __name__ == "__main__":
    # Define your CSV file paths
    gallery_csv_path = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviants_gallData_4_5_6/uniqueDev_gall_SnwBall03_6.2.csv.gz"
    metadata_csv_path = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_metaDataSnwBall/uniqueDev_metaData_SnwBall_02.csv.gz"

    # Create and start threads for loading
    gallery_thread = threading.Thread(target=load_gallery_data, args=(engine, gallery_csv_path))
    metadata_thread = threading.Thread(target=load_metadata, args=(engine, metadata_csv_path))

    gallery_thread.start()
    metadata_thread.start()

    print("Data loading for Gallery and MetaData started in separate threads.")
    print("You can continue to use the notebook and interact with other tables.")

    # Optional: If you need to wait for the loading to finish before proceeding
    # gallery_thread.join()
    # metadata_thread.join()
    # print("\nAll data loading threads finished.")