import os
import pandas as pd
import numpy as np
import re
import ast
import logging
from datetime import datetime
from dateutil import parser
import pytz
import threading

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Database Setup (unchanged) ---
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, BigInteger, Boolean, func, MetaData, Table, UniqueConstraint, inspect, text, select
from sqlalchemy.dialects import sqlite
import sqlalchemy

DATABASE_URL = 'sqlite:////mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_main05.db'
engine = create_engine(DATABASE_URL)
Base = declarative_base()
Session = sessionmaker(bind=engine)

# Your Artist, Watcher, Friend models (unchanged)
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
    watchers = relationship("Watcher", back_populates="artist")
    friends = relationship("Friend", back_populates="artist")
    __table_args__ = (UniqueConstraint('artist_name', name='_artist_name_uc'),)

class Watcher(Base):
    __tablename__ = 'watchers_v1'
    id = Column(Integer, primary_key=True)
    artist_id = Column(Integer, ForeignKey('artists.id'), nullable=False)
    watcher_name = Column(String, nullable=False)
    watcher_type = Column(String)
    is_watching = Column(Boolean)
    last_visit = Column(BigInteger)
    activity = Column(Boolean)
    collections = Column(Boolean)
    critiques = Column(Boolean)
    deviations = Column(Boolean)
    forum_threads = Column(Boolean)
    friend = Column(Boolean)
    journals = Column(Boolean)
    scraps = Column(Boolean)
    artist_name = Column(String)
    artist = relationship("Artist", back_populates="watchers_v1")
    __table_args__ = (UniqueConstraint('artist_id', 'watcher_name', name='_artist_watcher_uc'),)

class Friend(Base):
    __tablename__ = 'friends_v1'
    id = Column(Integer, primary_key=True)
    artist_id = Column(Integer, ForeignKey('artists.id'), nullable=False)
    friend_name = Column(String, nullable=False)
    friend_type = Column(String)
    is_watching = Column(Boolean)
    last_visit = Column(BigInteger)
    friends = Column(Boolean)
    deviations = Column(Boolean)
    journals = Column(Boolean)
    forum_threads = Column(Boolean)
    critiques = Column(Boolean)
    scraps = Column(Boolean)
    activity = Column(Boolean)
    collections = Column(Boolean)
    artist_name = Column(String)
    watches_you = Column(Boolean)
    artist = relationship("Artist", back_populates="friends_v1")
    __table_args__ = (UniqueConstraint('artist_id', 'friend_name', name='_artist_friend_uc'),)


Artist.watchers_v1 = relationship("Watcher", back_populates="artist")
Artist.friends_v1 = relationship("Friend", back_populates="artist")
#Create tables if they dont exist
Base.metadata.create_all(engine)

# --- Converters for pandas.read_csv (unchanged) ---
def bool_converter(x):
    if pd.isna(x) or not isinstance(x, str):
        return None
    return str(x).lower() == 'true'

def iso_date_to_ms(x):
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

 #   Column         Non-Null Count   Dtype Data columns (total 14 columns):
 # 0   Watchers name  888326 non-null  object
 # 1   user_icon      888326 non-null  object
  #2   type           888326 non-null  object
 # 3   is_watching    888326 non-null  bool
 # 4   last_visit     759781 non-null  object
 # 5   activity       888326 non-null  bool
 # 6   collections    888326 non-null  bool
 # 7   critiques      888326 non-null  bool
 # 8   deviations     888326 non-null  bool
 # 9   forum_threads  888326 non-null  bool
 # 10  friend         888326 non-null  bool
 # 11  journals       888326 non-null  bool
 # 12  scraps         888326 non-null  bool
 # 13  Deviant        888326 non-null  object

 # 0   Friends name   942998 non-null  object
 # 1   user_icon      942998 non-null  object
 # 2   type           942998 non-null  object
 # 3   is_watching    942998 non-null  bool
 # 4   watches_you    942998 non-null  bool
 # 5   last_visit     582454 non-null  object
 # 6   friends        942998 non-null  bool
 # 7   deviations     942998 non-null  bool
 # 8   journals       942998 non-null  bool
 # 9   forum_threads  942998 non-null  bool
 # 10  critiques      942998 non-null  bool
 # 11  scraps         942998 non-null  bool
 # 12  activity       942998 non-null  bool
 # 13  collections   942998 non-null  bool
 # 14  Deviant        942998 non-null  object

# --- Shared Dtype for CSV Reading ---
COMMON_COL_DTYPES = {
    'Watchers name': str, 'user_icon': str, 'type': str, 'Deviant': str,
    'Friends name': str,
}

COMMON_CONVERTERS = {
    'is_watching': bool_converter, 'activity': bool_converter, 'collections': bool_converter,
    'critiques': bool_converter, 'deviations': bool_converter, 'forum_threads': bool_converter,
    'friend': bool_converter, 'journals': bool_converter, 'scraps': bool_converter,
    'watches_you': bool_converter, 'last_visit': iso_date_to_ms
}

# --- Helper to get existing artist IDs once ---
def get_artist_name_to_id_map(session):
    logger.info("Fetching all existing artist names and IDs...")
    artist_map = {}
    for artist_id, artist_name in session.query(Artist.id, Artist.artist_name).all():
        artist_map[str(artist_name)] = artist_id
    logger.info(f"Loaded {len(artist_map)} existing artists.")
    return artist_map

# --- Tracking File Helpers ---
def get_last_processed_chunk(tracking_file_path):
    if not os.path.exists(tracking_file_path):
        return -1 # Start from the beginning if tracking file doesn't exist
    try:
        with open(tracking_file_path, 'r') as f:
            content = f.read().strip()
            if not content:
                return -1
            return int(content)
    except (ValueError, IOError) as e:
        logger.error(f"Error reading tracking file {tracking_file_path}: {e}")
        return -1 # Return -1 in case of error to be safe

def save_last_processed_chunk(tracking_file_path, chunk_index):
    try:
        with open(tracking_file_path, 'w') as f:
            f.write(str(chunk_index))
    except IOError as e:
        logger.error(f"Error writing to tracking file {tracking_file_path}: {e}")


# --- Optimized Incremental Load Function ---
def load_data_incrementally(csv_path, table_class, artist_name_col_csv, related_name_col_csv, chunksize=100000, artist_map_global=None, converters=None, temp_suffix='_temp', tracking_file_suffix='_last_chunk.txt'):
    if artist_map_global is None:
        raise ValueError("artist_map_global must be provided.")

    if not os.path.exists(csv_path):
        logger.error(f"CSV file not found: {csv_path}")
        return

    table_name = table_class.__tablename__
    tracking_file_path = f"{csv_path}{tracking_file_suffix}"
    last_processed_chunk_index = get_last_processed_chunk(tracking_file_path)
    logger.info(f"Starting incremental load for {table_name} from {csv_path}. Resuming from chunk {last_processed_chunk_index + 1}")


    total_rows_processed = 0
    total_skipped_artist = 0
    total_skipped_duplicates = 0
    temp_table_name = table_class.__tablename__ + temp_suffix
    main_table = table_class.__table__

    # Define a mapping from CSV column names to database column names for the current table
    csv_to_db_col_map = {}
    if table_class == Watcher:
        csv_to_db_col_map = {
            'Watchers name': 'watcher_name',
            # 'user_icon': 'user_icon', # Excluded as per user request
            'type': 'watcher_type',
            'is_watching': 'is_watching',
            'last_visit': 'last_visit',
            'activity': 'activity',
            'collections': 'collections',
            'critiques': 'critiques',
            'deviations': 'deviations',
            'forum_threads': 'forum_threads',
            'friend': 'friend',
            'journals': 'journals',
            'scraps': 'scraps',
            'Deviant': 'artist_name', # Mapping Deviant from CSV to artist_name in DB
            # 'artist_id' is handled separately
        }
    elif table_class == Friend:
        csv_to_db_col_map = {
            'Friends name': 'friend_name',
            # 'user_icon': 'user_icon', # Excluded as per user request
            'type': 'friend_type',
            'is_watching': 'is_watching',
            'watches_you': 'watches_you',
            'last_visit': 'last_visit',
            'friends': 'friends',
            'deviations': 'deviations',
            'journals': 'journals',
            'forum_threads': 'forum_threads',
            'critiques': 'critiques',
            'scraps': 'scraps',
            'activity': 'activity',
            'collections': 'collections',
            'Deviant': 'artist_name', # Mapping Deviant from CSV to artist_name in DB
            # 'artist_id' is handled separately
        }
    else:
        logger.error(f"Unknown table class: {table_class.__name__}. Cannot define column mapping.")
        return


    logger.info(f"Starting incremental load for {table_class.__tablename__} from {csv_path}")

    try:
        for i, chunk in enumerate(pd.read_csv(
            csv_path,
            chunksize=chunksize,
            on_bad_lines='skip',
            dtype=COMMON_COL_DTYPES,
            converters=converters # Use the provided converters
        )):
            # Skip chunks that have already been processed
            if i <= last_processed_chunk_index:
                logger.info(f"  Skipping already processed chunk {i + 1} for {table_name}")
                continue

            logger.info(f"Processing chunk {i + 1} for {table_class.__tablename__} (rows read: {len(chunk)})")

            # --- 1. Map artist_name to artist_id ---
            if artist_name_col_csv not in chunk.columns:
                logger.error(f"  Missing required column '{artist_name_col_csv}' in CSV chunk {i+1} for {table_class.__tablename__}. Skipping chunk.")
                continue

            chunk['artist_id'] = chunk[artist_name_col_csv].map(artist_map_global)

            unmapped_artists_df = chunk[chunk['artist_id'].isna()]
            if not unmapped_artists_df.empty:
                logger.warning(f"  Skipping {len(unmapped_artists_df)} rows in chunk {i+1} due to unmapped artist names. Examples: {unmapped_artists_df[artist_name_col_csv].unique()[:5].tolist()}")
                total_skipped_artist += len(unmapped_artists_df)


            chunk_mapped = chunk.dropna(subset=['artist_id']).copy() # Create a copy to avoid SettingWithCopyWarning
            if chunk_mapped.empty:
                logger.info(f"  Chunk {i+1} became empty after filtering unmapped artists.")
                save_last_processed_chunk(tracking_file_path, i) # Save progress even if chunk is empty after filtering
                continue

            chunk_mapped['artist_id'] = chunk_mapped['artist_id'].astype(int)
            logger.info(f"  Rows after artist mapping: {len(chunk_mapped)}")

            # --- 2. Create DataFrame for insertion, matching SQLA model attributes ---
            df_to_insert = pd.DataFrame()

            df_to_insert['artist_id'] = chunk_mapped['artist_id']

            # Use the defined mapping to select and rename columns
            for csv_col, db_col in csv_to_db_col_map.items():
                if csv_col in chunk_mapped.columns:
                    df_to_insert[db_col] = chunk_mapped[csv_col]
                else:
                    # Handle cases where a CSV column expected by the map is missing
                    # For boolean columns, default to False if not in CSV
                    if db_col in [c.name for c in main_table.columns if isinstance(c.type, Boolean)]:
                         df_to_insert[db_col] = False
                         logger.warning(f"  CSV column '{csv_col}' not found in chunk {i+1} for DB column '{db_col}', defaulting to False.")
                    else:
                         df_to_insert[db_col] = None
                         logger.warning(f"  CSV column '{csv_col}' not found in chunk {i+1} for DB column '{db_col}', defaulting to None.")


            # --- Add step to drop duplicates within the chunk DataFrame ---
            # Determine the columns for dropping duplicates based on the unique constraint
            unique_cols_main_names = []
            try:
                unique_constraint_cols = table_class.__table_args__[0].columns
                unique_cols_main_names = [col.name for col in unique_constraint_cols]
            except (AttributeError, IndexError):
                 logger.error(f"  Could not determine unique constraint columns for {table_class.__tablename__} to drop duplicates within chunk.")
                 # Continue without dropping duplicates if unique constraint cols can't be determined


            if unique_cols_main_names and all(col_name in df_to_insert.columns for col_name in unique_cols_main_names):
                initial_rows_in_df = len(df_to_insert)
                df_to_insert.drop_duplicates(subset=unique_cols_main_names, inplace=True)
                rows_dropped_in_df = initial_rows_in_df - len(df_to_insert)
                if rows_dropped_in_df > 0:
                    logger.warning(f"  Dropped {rows_dropped_in_df} duplicate rows within chunk {i+1} DataFrame based on {unique_cols_main_names}.")
                    total_skipped_duplicates += rows_dropped_in_df # Count these as skipped duplicates
            elif unique_cols_main_names:
                 logger.warning(f"  Could not find all unique constraint columns {unique_cols_main_names} in df_to_insert for dropping duplicates within chunk {i+1}.")


            # --- 3. Bulk insert to temporary table ---
            with engine.connect() as conn:
                with conn.begin():
                    try:
                        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
                        logger.info(f"  Dropped temporary table {temp_table_name}")
                    except Exception as drop_e:
                        logger.warning(f"  Could not drop temporary table {temp_table_name}: {drop_e}")


                    sqla_dtype_map = {col.name: col.type for col in main_table.columns if col.name != 'id'}

                    try:
                        # Ensure column order matches the target table exactly before to_sql
                        # Get the list of column names from the main table (excluding 'id')
                        target_column_order = [c.name for c in main_table.columns if c.name != 'id']
                        # Reindex the DataFrame to match the target column order, filling missing columns with None
                        df_to_insert = df_to_insert.reindex(columns=target_column_order)

                        df_to_insert.to_sql(temp_table_name, conn, if_exists='replace', index=False, dtype=sqla_dtype_map)
                        rows_in_temp = len(df_to_insert)
                        logger.info(f"  Inserted {rows_in_temp} rows into temporary table {temp_table_name}")
                    except Exception as to_sql_e:
                        logger.error(f"  Error inserting into temporary table {temp_table_name}: {to_sql_e}", exc_info=True)
                        continue # Skip to next chunk if temp insert fails


                    # --- 4. Atomic insert from temp to main table with WHERE NOT EXISTS ---
                    cols_to_insert = [c.name for c in main_table.columns if c.name != main_table.primary_key.columns.keys()[0]]

                    # Important: Reflect the temporary table to get its Columns for the SELECT statement
                    # Need to ensure metadata is up-to-date for reflection
                    temp_metadata = MetaData()
                    # Use only= parameter to specify the temporary table name for reflection
                    temp_metadata.reflect(bind=conn, only=[temp_table_name])
                    temp_table = temp_metadata.tables[temp_table_name]


                    # Build select list where columns are from the temp_table.c (columns)
                    select_temp_cols = [temp_table.c[col_name] for col_name in cols_to_insert if col_name in temp_table.c]

                    # Build the WHERE NOT EXISTS clause based on the unique constraint columns
                    # The unique constraint is assumed to be the first one in __table_args__
                    try:
                        unique_cols_main_names = [col.name for col in table_class.__table_args__[0].columns]
                        # Ensure unique columns exist in both main and temporary tables before building the WHERE clause
                        if all(col_name in main_table.c and col_name in temp_table.c for col_name in unique_cols_main_names):
                            unique_cols_main = [main_table.c[col_name] for col_name in unique_cols_main_names]
                            unique_cols_temp = [temp_table.c[col_name] for col_name in unique_cols_main_names]

                            # Subquery to check for existence
                            exists_subquery = select(unique_cols_main[0]) \
                                             .where(unique_cols_main[0] == unique_cols_temp[0]) \
                                             .where(unique_cols_main[1] == unique_cols_temp[1]) \
                                             .exists()

                            # The final INSERT FROM SELECT statement
                            insert_stmt = sqlite.insert(main_table).from_select(
                                [c.name for c in select_temp_cols], # Ensure column names match for from_select
                                select(*select_temp_cols).where(~exists_subquery) # Only select rows that do NOT exist in main_table
                            )
                        else:
                            logger.error(f"  Unique constraint columns {unique_cols_main_names} not found in both main and temporary tables for {table_class.__tablename__}. Cannot build WHERE NOT EXISTS clause.")
                            # Fallback to a simple insert (will likely fail on duplicates unless tables were dropped)
                            insert_stmt = sqlite.insert(main_table).from_select(
                                [c.name for c in select_temp_cols],
                                select(*select_temp_cols)
                            )

                    except (AttributeError, IndexError):
                         logger.error(f"  Could not determine unique constraint columns for {table_class.__tablename__}. Skipping insert from temp.")
                         continue


                    logger.info(f"  Generated INSERT statement: {insert_stmt.compile(engine)}")

                    try:
                        result = conn.execute(insert_stmt)
                        rows_inserted = result.rowcount
                        logger.info(f"  Inserted {rows_inserted} new rows into {main_table.name} from temp table.")

                        rows_in_temp = len(df_to_insert)
                        rows_skipped_in_this_chunk = rows_in_temp - rows_inserted
                        # total_skipped_duplicates already includes duplicates dropped within the chunk DataFrame

                        if rows_skipped_in_this_chunk > 0:
                            logger.warning(f"  Skipped {rows_skipped_in_this_chunk} existing {table_class.__tablename__} records in chunk {i+1} during insert from temp (already in main table).")

                        total_rows_processed += rows_inserted

                        # --- Save progress after successful chunk processing ---
                        save_last_processed_chunk(tracking_file_path, i)

                    except Exception as insert_e:
                        logger.error(f"  Error inserting from temporary table to main table {main_table.name}: {insert_e}", exc_info=True)
                        # Reraise the exception to stop processing if an integrity error occurs
                        raise insert_e


    except Exception as e:
        logger.error(f"Error processing {table_class.__tablename__} chunk: {e}", exc_info=True)
        # If the error is an IntegrityError, it will be logged above and the process will stop due to the re-raise
        # For other errors, log and continue to the next file if applicable
        if not isinstance(e, sqlalchemy.exc.IntegrityError):
             logger.error(f"Continuing with the next file/step after error in {table_class.__tablename__}: {e}")


    logger.info(f"Finished loading {table_class.__tablename__}. Total processed: {total_rows_processed} rows. Total skipped unmapped artists: {total_skipped_artist} rows. Total skipped duplicates: {total_skipped_duplicates} rows.")

# --- Main Execution Function ---
def main():
    # Ensure tables exist
    Base.metadata.create_all(engine)
    with Session() as session:
        artist_name_to_id = get_artist_name_to_id_map(session)
        watchers_csv = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_wtchrsSnwball_fin1.csv.gz"
        friends_csv = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_friendsSnwball_fin.csv.gz"

        # --- Removed Drop and Recreate Table Logic ---

        logger.info("\nStarting Friends data load.")
        try:
            load_data_incrementally(
                csv_path=friends_csv,
                table_class=Friend,
                artist_name_col_csv='Deviant',
                related_name_col_csv='Friends name',
                artist_map_global=artist_name_to_id,
                converters=COMMON_CONVERTERS
            )
        except sqlalchemy.exc.IntegrityError as e:
             logger.error(f"IntegrityError occurred during Friends data load: {e}")
             # Decide whether to stop or continue based on your needs
             # For now, let's stop to investigate
             logger.info("Stopping data load due to IntegrityError in Friends data.")
             # No exit() here, allow the main thread to finish


        logger.info("\nStarting Watchers data load.")
        try:
            load_data_incrementally(
                csv_path=watchers_csv,
                table_class=Watcher,
                artist_name_col_csv='Deviant',
                related_name_col_csv='Watchers name',
                artist_map_global=artist_name_to_id,
                converters=COMMON_CONVERTERS
            )
        except sqlalchemy.exc.IntegrityError as e:
             logger.error(f"IntegrityError occurred during Watchers data load: {e}")
             logger.info("Stopping data load due to IntegrityError in Watchers data.")
             # No need to exit here as it's the last step, but you could add a flag if needed

    logger.info("\nDatabase loading complete.")

if __name__ == "__main__":
    # This part will be modified in the next step to use threading
    main()