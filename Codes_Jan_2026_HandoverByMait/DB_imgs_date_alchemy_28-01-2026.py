# Standard library imports
import re, ast
import threading # Import the threading module
import logging # Import the logging module

# Third-party library imports
import pandas as pd
import numpy as np

# SQLAlchemy imports
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, BigInteger, Boolean, func, MetaData, Table, UniqueConstraint, inspect, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.exc import SQLAlchemyError

# --- Database Setup ---
# Define the SQLite database URL. This will create a file at the specified path.
DATABASE_URL =  'sqlite:////mnt/hdd/maittewa/deviantArt_DeviantData/dbData/deviantArt_main05.db'
engine = create_engine(DATABASE_URL)  # Create a database engine
Base = declarative_base() # Base class for declarative models
Session = sessionmaker(bind=engine) # Session factory for database interactions

# Configure Logging specifically for this cell/thread to capture its output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- SQLAlchemy Models: Define database tables as Python classes ---

class Artist(Base):
    # Table name in the database
    __tablename__ = 'artists'
    # Columns definition (matching the existing Artist table structure)
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
    
    # Relationship definition: One-to-many with imgs_date
    imgs_date = relationship("imgs_date", back_populates="artist")


class imgs_date(Base):
    # Table name in the database
    __tablename__ = 'imgs_date'
    # Columns definition
    id = Column(String, primary_key=True) # Primary key for the image deviation ID
    artist_id = Column(Integer, ForeignKey('artists.id'))  # Foreign key linking to Artist.id
    artist_name = Column(String) # Denormalized artist name for convenience
    date = Column(BigInteger) # Unix timestamp in milliseconds for published date

    # Relationship to the Artist table
    # 'order_by=artist_id' can help with consistent ordering if needed, but might not be strictly necessary for back_populates
    artist = relationship("Artist", order_by=artist_id, back_populates="imgs_date")

    # Table arguments, including unique constraints
    __table_args__ = (
        UniqueConstraint('id', name='unique_imgs_date_id'), # Ensure image deviation IDs are unique
    )

# --- Data Saving Function ---
def save_imgs_date_incrementally(engine, image_date_data_path):
    """Saves imgs_date data incrementally from a CSV file into the database.
    Skips existing records and updates artist_name if it has changed.
    """
    Session = sessionmaker(bind=engine) # Create a new session for this function
    session = Session()
    # Initialize counters for tracking progress
    total_rows_saved = 0
    total_rows_updated = 0
    total_rows_skipped = 0
    total_rows_processed = 0

    try:
        # Create tables if they don't exist (ensures imgs_date table is present)
        Base.metadata.create_all(engine)
        logger.info("Ensured imgs_date table exists.")

        # Read CSV in chunks to handle potentially large files efficiently
        for chunk in pd.read_csv(image_date_data_path, chunksize=1000, on_bad_lines='skip'):
            for index, row in chunk.iterrows():
                deviation_id = row['Deviation_id']
                author_name_csv = row['Author_name']
                published_on = row['Published_on']

                # Find the corresponding artist in the 'artists' table
                artist = session.query(Artist).filter_by(artist_name=author_name_csv).first()

                if artist:
                    artist_id = artist.id
                    artist_name = artist.artist_name

                    # Check if a record for this deviation ID already exists in imgs_date
                    existing_date_record = session.query(imgs_date).filter_by(id=deviation_id).first()

                    if existing_date_record:
                        # If record exists, check if 'artist_name' needs an update
                        if existing_date_record.artist_name is None or existing_date_record.artist_name != artist_name:
                            existing_date_record.artist_name = artist_name
                            session.add(existing_date_record) # Add to session for update
                            total_rows_updated += 1
                            logger.info(f"Updated artist_name for imgs_date record with id: {deviation_id}")
                        else:
                            total_rows_skipped += 1
                            # Log for skipped existing records (optional, can be very verbose)
                            # logger.info(f"Skipping existing imgs_date record with id: {deviation_id} (artist_name already correct)")
                    else:
                        # If record does not exist, create a new one
                        new_image_date = imgs_date(
                            id=deviation_id,
                            artist_id=artist_id,
                            date=published_on,
                            artist_name=artist_name # Populate artist_name from the Artist table
                        )
                        session.add(new_image_date)
                        total_rows_saved += 1
                        logger.info(f"Saved new imgs_date record: {new_image_date.id}")

                    try:
                        session.commit()  # Commit after each row for incremental saving (can be batched for performance)
                        total_rows_processed += 1
                    except SQLAlchemyError as e:
                        session.rollback() # Rollback on error to prevent inconsistent state
                        logger.error(f"Error saving/updating record {deviation_id}: {e}")
                        # Further error handling (e.g., logging to a separate file, skipping bad rows) could be added here
                else:
                    logger.warning(f"Artist not found in 'artists' table for image date with ID: {deviation_id} (Author_name: {author_name_csv})")

    except Exception as e:
        logger.error(f"An unhandled error occurred during incremental save: {e}", exc_info=True)
    finally:
        session.close() # Ensure the session is closed
        logger.info(f"Incremental save complete. Total processed rows from CSV: {total_rows_processed}. New rows saved: {total_rows_saved}. Existing rows updated: {total_rows_updated}. Existing rows skipped: {total_rows_skipped}.")

# --- Wrapper function for threading ---
def run_save_imgs_date(engine, image_date_data_path):
    """Wrapper to run the incremental save function, catching any top-level exceptions.
    Designed to be used as a target for a separate thread.
    """
    try:
        save_imgs_date_incrementally(engine, image_date_data_path)
    except Exception as e:
        logger.error(f"An error occurred in the saving thread: {e}", exc_info=True)

# --- Main Execution Block ---
# This block will only execute when the script is run directly, not when imported.
if __name__ == "__main__":
    # Define the path to the CSV file containing image date data
    image_date_data_path = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviants_gallData_4_5_6/uniqueDev_gall_SnwBall03_6.2.csv.gz"

    # Create and start the data saving thread
    data_saving_thread = threading.Thread(target=run_save_imgs_date, args=(engine, image_date_data_path))
    data_saving_thread.start()

    print("Image date saving started in a separate thread. You can now interact with other parts of the notebook.")

    # Wait for the saving thread to finish. This ensures all logs are displayed before the cell finishes execution.
    data_saving_thread.join()
    print("\nImage date saving thread finished.")
