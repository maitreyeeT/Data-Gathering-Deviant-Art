import os
import pandas as pd
import numpy as np
import re
import ast
import logging
from datetime import datetime
from dateutil import parser
import pytz
from collections import defaultdict
import threading 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, BigInteger, Boolean, func, MetaData, Table, UniqueConstraint, inspect, text, select
from sqlalchemy.dialects import sqlite

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Database Setup ---
DATABASE_URL = 'sqlite:////mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_main05.db'
engine = create_engine(DATABASE_URL)
Base = declarative_base()
Session = sessionmaker(bind=engine)

# --- Global Interactor ID Mapping (in-memory for this run) ---
global_interactor_name_to_id = {}
global_interactor_id_counter = 1

def get_or_create_interactor_id(interactor_name):
    global global_interactor_id_counter
    if interactor_name not in global_interactor_name_to_id:
        global_interactor_name_to_id[interactor_name] = global_interactor_id_counter
        global_interactor_id_counter += 1
    return global_interactor_name_to_id[interactor_name]

# --- Model Definitions ---
class Artist(Base):
    __tablename__ = 'artists'
    id = Column(Integer, primary_key=True)
    artist_name = Column(String, unique=True, index=True)
    profile_url = Column(String)
    country = Column(String)
    level = Column(String)
    registration_date = Column(Integer)
    no_of_deviations = Column(Integer)
    no_of_favourites = Column(Integer)
    no_of_user_comments = Column(Integer)
    no_of_pageviews = Column(Integer)
    no_of_profile_comments = Column(Integer)
    is_artist = Column(Boolean)
    gender = Column(String)
    speciality = Column(String)
    no_of_images = Column(Integer)
    no_of_AI_images = Column(Integer)
    ai_adopter = Column(Boolean)
    ai_adoption_first_time = Column(Integer)
    interactions = relationship("ArtistInteraction", back_populates="artist_to")
    # NEW: Relationship for the 'Watching' table
    watching = relationship("Watching", back_populates="artist")


# NEW: The Watching table model
class Watching(Base):
    __tablename__ = 'watchings'
    id = Column(Integer, primary_key=True)
    artist_id = Column(Integer, ForeignKey('artists.id'), nullable=False) # Foreign key to Artist table
    watching_name = Column(String, nullable=False) # Name of the artist being watched by 'artist_id'
    
    artist = relationship("Artist", back_populates="watching")
    
    __table_args__ = (
        UniqueConstraint('artist_id', 'watching_name', name='_artist_watching_uc_orig'), # Original constraint from Watchers table
    )


# 4. Define ArtistInteraction table
class ArtistInteraction(Base):
    __tablename__ = 'artist_interactions'
    id = Column(Integer, primary_key=True)
    artist_id_to = Column(Integer, ForeignKey('artists.id'), nullable=False) # The Deviant who is being watched/friended/watched-by
    artist_name_to = Column(String, nullable=False)

    artist_id_from = Column(BigInteger, nullable=False) # The unique ID of the interactor (watcher/friend/watching target)
    artist_name_from = Column(String, nullable=False) # The name of the interactor

    interaction_type = Column(String, nullable=False) # 'watcher', 'watching', 'friend' (tag)
    date = Column(BigInteger, nullable=True) # Unix timestamp in milliseconds

    artist_to = relationship("Artist", back_populates="interactions")

    __table_args__ = (
        # Unique constraint to prevent duplicate edges: (who it's *to*, who it's *from*, and what *type* of interaction)
        UniqueConstraint('artist_id_to', 'artist_id_from', 'interaction_type', name='_interaction_edge_uc'),
    )

# Create tables (if they don't exist)
Base.metadata.create_all(engine)

# --- Converters for pandas.read_csv ---
def bool_converter(x):
    """Converts string 'True'/'False' to boolean, handles None/NaN."""
    if pd.isna(x) or not isinstance(x, str):
        return None
    return str(x).lower() == 'true'

def iso_date_to_ms(x):
    """Converts ISO 8601 date string to Unix timestamp in milliseconds.
    Uses dateutil.parser for robust parsing. Returns None on failure.
    """
    if pd.isna(x):
        return None
    if not isinstance(x, str) or not x.strip():
        return None
    try:
        dt_obj = parser.parse(x)
        if dt_obj.tzinfo is None:
             dt_obj = pytz.utc.localize(dt_obj)
        else:
            dt_obj = dt_obj.astimezone(pytz.utc)
        return int(dt_obj.timestamp() * 1000)
    except Exception as e:
        logger.warning(f"Could not convert date string '{x}' to timestamp. Error: {e}. Setting to None.")
        return None

# --- Shared Dtype for CSV Reading (for ALL CSVs containing these columns) ---
COMMON_COL_DTYPES = {
    'user_icon': str, 'watcher_type': str, 'artist_name': str,
    'Friend Name': str, 'Deviant': str, 'Watchers name': str,
    'Watching Name': str # New column for 'watching' CSV, if still reading from it
}

COMMON_CONVERTERS = {
    'is_watching': bool_converter, 'activity': bool_converter, 'collections': bool_converter,
    'critiques': bool_converter, 'deviations': bool_converter, 'forum_threads': bool_converter,
    'friend': bool_converter, 'journals': bool_converter, 'scraps': bool_converter,
    'watches_you': bool_converter,
    'last_visit': iso_date_to_ms
}

# --- Helper to get existing artist IDs once (for artist_id_to) ---
def get_deviant_name_to_id_map(session):
    """Fetches all artist_name to id mappings from the 'artists' table."""
    logger.info("Fetching all existing deviant names and IDs...")
    deviant_map = {}
    for artist_id, artist_name in session.query(Artist.id, Artist.artist_name).all():
        deviant_map[str(artist_name)] = artist_id
    logger.info(f"Loaded {len(deviant_map)} existing deviants.")
    return deviant_map

# --- Load Interactions from CSV (for Watchers and Friends) ---
def load_interactions_from_csv(csv_path, interaction_type, deviant_col_csv, from_col_csv, date_col_csv=None, chunksize=100000, deviant_map_global=None):
    """
    Loads interactions from a CSV.
    - Handles mapping of 'Deviant' to artist_id_to and artist_name_to from global map.
    - Assigns unique IDs to interactors based on global interactor map for artist_id_from.
    - Uses a temporary table and WHERE NOT EXISTS for duplicate handling.
    """
    if deviant_map_global is None:
        raise ValueError("deviant_map_global must be provided.")

    total_rows_processed = 0
    total_skipped_deviant = 0
    total_skipped_duplicates = 0
    temp_table_name = "artist_interactions_temp"
    main_table = ArtistInteraction.__table__

    logger.info(f"Starting incremental CSV load for interaction_type '{interaction_type}' from {csv_path}")

    try:
        for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunksize, on_bad_lines='skip', dtype=COMMON_COL_DTYPES, converters=COMMON_CONVERTERS)):
            logger.info(f"Processing chunk {i + 1} for '{interaction_type}' (rows {total_rows_processed + 1} to {total_rows_processed + len(chunk)})")

            # --- Map Deviant (artist_id_to, artist_name_to) ---
            if deviant_col_csv not in chunk.columns:
                logger.error(f"  Missing required column '{deviant_col_csv}' in CSV chunk {i+1}. Skipping chunk.")
                continue

            chunk['artist_id_to'] = chunk[deviant_col_csv].map(deviant_map_global)
            chunk['artist_name_to'] = chunk[deviant_col_csv]

            unmapped_deviants_df = chunk[chunk['artist_id_to'].isna()]
            if not unmapped_deviants_df.empty:
                logger.warning(f"  Skipping {len(unmapped_deviants_df)} rows in chunk {i+1} due to unmapped deviant names (not found in 'artists' table). Examples: {unmapped_deviants_df[deviant_col_csv].unique()[:5].tolist()}")
                total_skipped_deviant += len(unmapped_deviants_df)
                chunk = chunk.dropna(subset=['artist_id_to'])

            if chunk.empty:
                logger.info(f"  Chunk {i+1} became empty after filtering unmapped deviants.")
                continue

            chunk['artist_id_to'] = chunk['artist_id_to'].astype(int)

            # --- Map Interactor (artist_id_from, artist_name_from) ---
            if from_col_csv not in chunk.columns:
                logger.error(f"  Missing required column '{from_col_csv}' in CSV chunk {i+1}. Skipping chunk.")
                continue

            chunk['artist_name_from'] = chunk[from_col_csv]
            chunk['artist_id_from'] = chunk['artist_name_from'].apply(get_or_create_interactor_id)


            # --- Prepare DataFrame for insertion ---
            df_to_insert = pd.DataFrame()
            df_to_insert['artist_id_to'] = chunk['artist_id_to']
            df_to_insert['artist_name_to'] = chunk['artist_name_to']
            df_to_insert['artist_id_from'] = chunk['artist_id_from']
            df_to_insert['artist_name_from'] = chunk['artist_name_from']
            df_to_insert['interaction_type'] = interaction_type
            
            if date_col_csv and date_col_csv in chunk.columns:
                df_to_insert['date'] = chunk[date_col_csv]
            else:
                df_to_insert['date'] = None

            # --- Bulk insert to temporary table & then to main table ---
            with engine.connect() as conn:
                with conn.begin():
                    conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
                    
                    sqla_dtype_map = {col.name: col.type for col in ArtistInteraction.__table__.columns if col.name != 'id'}
                    df_to_insert.to_sql(temp_table_name, conn, if_exists='replace', index=False, dtype=sqla_dtype_map)
                    
                    # --- Insert from temp to main with WHERE NOT EXISTS ---
                    cols_to_insert = [c.name for c in main_table.columns if c.name != main_table.primary_key.columns.keys()[0]]

                    temp_table = Table(temp_table_name, Base.metadata, autoload_with=conn)
                    select_temp_cols = [temp_table.c[col_name] for col_name in cols_to_insert]
                    
                    unique_cols_main = [main_table.c['artist_id_to'], main_table.c['artist_id_from'], main_table.c['interaction_type']]
                    unique_cols_temp = [temp_table.c['artist_id_to'], temp_table.c['artist_id_from'], temp_table.c['interaction_type']]
                    
                    exists_subquery = select(unique_cols_main[0]) \
                                     .where(unique_cols_main[0] == unique_cols_temp[0]) \
                                     .where(unique_cols_main[1] == unique_cols_temp[1]) \
                                     .where(unique_cols_main[2] == unique_cols_temp[2]) \
                                     .exists()
                    
                    insert_stmt = sqlite.insert(main_table).from_select(
                        cols_to_insert,
                        select(*select_temp_cols).where(~exists_subquery)
                    )
                    
                    result = conn.execute(insert_stmt)
                    rows_inserted = result.rowcount

                    rows_in_temp = len(df_to_insert)
                    rows_skipped_in_this_chunk = rows_in_temp - rows_inserted
                    total_skipped_duplicates += rows_skipped_in_this_chunk

                    if rows_skipped_in_this_chunk > 0:
                        logger.warning(f"  Skipped {rows_skipped_in_this_chunk} existing {interaction_type} records in chunk {i+1} due to duplicate keys.")
                    
                    total_rows_processed += rows_inserted

    except Exception as e:
        logger.error(f"Error processing {interaction_type} chunk from {csv_path}: {e}", exc_info=True)
    
    logger.info(f"Finished loading '{interaction_type}'. Total processed: {total_rows_processed} rows. Total skipped unmapped deviants: {total_skipped_deviant} rows. Total skipped duplicates: {total_skipped_duplicates} rows.")


# --- NEW FUNCTION: Load Interactions from Watching DB Table ---
def load_watchings_from_db(session, deviant_map_global, batch_size=10000):
    """
    Loads 'watching' interactions directly from the 'watchings' database table.
    - Maps existing 'Watching' entries to 'ArtistInteraction' entries.
    - Assigns unique IDs to watched artists for artist_id_from.
    - Handles pagination for large tables and duplicate checking.
    """
    if deviant_map_global is None:
        raise ValueError("deviant_map_global must be provided.")

    total_rows_processed = 0
    total_skipped_deviant = 0 # Deviants not found in artists table
    total_skipped_duplicates = 0
    temp_table_name = "artist_interactions_temp"
    main_table = ArtistInteraction.__table__

    logger.info("Starting incremental DB load for interaction_type 'watching' from 'watchings' table.")

    try:
        offset = 0
        while True:
            # Fetch a batch of Watching records, joining with Artist to get artist_name
            # for `artist_name_to` and `deviant_map_global` lookup.
            batch = session.query(Watching.artist_id, Artist.artist_name, Watching.watching_name) \
                           .join(Artist, Artist.id == Watching.artist_id) \
                           .offset(offset) \
                           .limit(batch_size) \
                           .all()

            if not batch:
                break # No more records

            logger.info(f"  Processing batch {offset // batch_size + 1} for 'watching' (rows {offset + 1} to {offset + len(batch)})")

            df_to_insert_list = []
            
            for watching_artist_id, watching_artist_name, watching_target_name in batch:
                # --- Map Deviant (artist_id_to, artist_name_to) ---
                # Check if the 'artist_id' from Watching table actually exists in our deviant_map_global
                # It *should* if it came from Artists table, but a defensive check is good.
                artist_id_to = watching_artist_id
                artist_name_to = watching_artist_name
                
                if artist_id_to not in deviant_map_global.values(): # Or, more robustly, check existence in session
                    logger.warning(f"    Skipping watching interaction for deviant_id {artist_id_to} ('{artist_name_to}') as their name is not in the global deviant map. This should not happen if data is consistent.")
                    total_skipped_deviant += 1
                    continue

                # --- Map Interactor (artist_id_from, artist_name_from) ---
                artist_name_from = watching_target_name
                artist_id_from = get_or_create_interactor_id(artist_name_from)

                # --- Prepare row for insertion ---
                df_to_insert_list.append({
                    'artist_id_to': artist_id_to,
                    'artist_name_to': artist_name_to,
                    'artist_id_from': artist_id_from,
                    'artist_name_from': artist_name_from,
                    'interaction_type': 'watching',
                    'date': None # As requested, no date for watching
                })
            
            if not df_to_insert_list: # If all rows in batch were skipped
                offset += len(batch)
                continue

            df_to_insert = pd.DataFrame(df_to_insert_list)

            # --- Bulk insert to temporary table & then to main table (same logic as CSV loader) ---
            with engine.connect() as conn:
                with conn.begin():
                    conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
                    
                    sqla_dtype_map = {col.name: col.type for col in ArtistInteraction.__table__.columns if col.name != 'id'}
                    df_to_insert.to_sql(temp_table_name, conn, if_exists='replace', index=False, dtype=sqla_dtype_map)
                    
                    cols_to_insert = [c.name for c in main_table.columns if c.name != main_table.primary_key.columns.keys()[0]]

                    temp_table = Table(temp_table_name, Base.metadata, autoload_with=conn)
                    select_temp_cols = [temp_table.c[col_name] for col_name in cols_to_insert]
                    
                    unique_cols_main = [main_table.c['artist_id_to'], main_table.c['artist_id_from'], main_table.c['interaction_type']]
                    unique_cols_temp = [temp_table.c['artist_id_to'], temp_table.c['artist_id_from'], temp_table.c['interaction_type']]
                    
                    exists_subquery = select(unique_cols_main[0]) \
                                     .where(unique_cols_main[0] == unique_cols_temp[0]) \
                                     .where(unique_cols_main[1] == unique_cols_temp[1]) \
                                     .where(unique_cols_main[2] == unique_cols_temp[2]) \
                                     .exists()
                    
                    insert_stmt = sqlite.insert(main_table).from_select(
                        cols_to_insert,
                        select(*select_temp_cols).where(~exists_subquery)
                    )
                    
                    result = conn.execute(insert_stmt)
                    rows_inserted = result.rowcount

                    rows_in_temp = len(df_to_insert)
                    rows_skipped_in_this_chunk = rows_in_temp - rows_inserted
                    total_skipped_duplicates += rows_skipped_in_this_chunk

                    if rows_skipped_in_this_chunk > 0:
                        logger.warning(f"  Skipped {rows_skipped_in_this_chunk} existing 'watching' records in batch {offset // batch_size + 1} due to duplicate keys.")
                    
                    total_rows_processed += rows_inserted
            
            offset += len(batch) # Move to the next batch

    except Exception as e:
        logger.error(f"Error processing 'watching' interactions from DB table: {e}", exc_info=True)
    
    logger.info(f"Finished loading 'watching' interactions from DB. Total processed: {total_rows_processed} rows. Total skipped unmapped deviants: {total_skipped_deviant} rows. Total skipped duplicates: {total_skipped_duplicates} rows.")

def run_data_loading(engine):
    
    with Session() as session:
        deviant_name_to_id = get_deviant_name_to_id_map(session)

        # Your CSV file paths
        #watchers_csv = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_wtchrsSnwball_fin1.csv.gz"
        friends_csv = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_friendsSnwball_fin.csv.gz"
        
        # NOTE: Since we are loading WATCHINGS *from the DB table*,
        # you no longer need the 'watching_csv' path here, and you should ensure
        # that the 'watchings' table is already populated from wherever its data came.

       
        # --- Load Watching from DB Table ---
        #logger.info("\n--- Starting Watching data load from 'watchings' DB table ---")
        #load_watchings_from_db(session, deviant_map_global=deviant_name_to_id)


     # --- Load Watchers from CSV ---
      #  logger.info("--- Starting Watchers data load from CSV ---")
      #  load_interactions_from_csv(
      #      csv_path=watchers_csv,
      #      interaction_type='watcher',
      #      deviant_col_csv='Deviant',
      #      from_col_csv='Watchers name',
      #      date_col_csv='last_visit',
      #      deviant_map_global=deviant_name_to_id
      #  )

        # --- Load Friends from CSV ---
        logger.info("\n--- Starting Friends data load from CSV ---")
        load_interactions_from_csv(
            csv_path=friends_csv,
            interaction_type='friend',
            deviant_col_csv='Deviant',
            from_col_csv='Friends name',
            date_col_csv='last_visit',
            deviant_map_global=deviant_name_to_id
        )


    logger.info("\nDatabase interaction loading complete.")

# --- Main Execution ---
if __name__ == "__main__":
    # Ensure tables exist (including the new Watching table)
    Base.metadata.create_all(engine)

    # Create and start the data loading thread
    data_loading_thread = threading.Thread(target=run_data_loading, args=(engine,))
    data_loading_thread.start()

    print("Data loading started in a separate thread. You can now interact with other tables.")

   
