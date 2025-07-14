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

from sqlalchemy import text

DATABASE_URL =  'sqlite:////mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_main05.db'
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
    #Relationship description
    imgs_date = relationship("imgs_date", back_populates="artist")  # Establish relationship from Artist to images date


#Images date class
class imgs_date(Base):
    __tablename__ = 'imgs_date'
    id = Column(String, primary_key=True)
    artist_id = Column(Integer, ForeignKey('artists.id'))  # Link to Artist table
    artist_name = Column(String)
    date = Column(BigInteger)

    artist = relationship("Artist", order_by=artist_id, back_populates="imgs_date")  # Define the relationship

    # Add UniqueConstraint for the 'id' column
    __table_args__ = (
        UniqueConstraint('id', name='unique_imgs_date_id'),
    )
def save_imgs_date_incrementally(engine, image_date_data_path):
    """Saves imgs_date data incrementally, skipping existing records and updating artist_name."""
    Session = sessionmaker(bind=engine)
    session = Session()
    # Initialize counters before the try block
    total_rows_saved = 0
    total_rows_updated = 0
    total_rows_skipped = 0
    total_rows_processed = 0


    try:
        # Create tables if they don't exist
        Base.metadata.create_all(engine)

        for chunk in pd.read_csv(image_date_data_path, chunksize=1000, on_bad_lines='skip'):  # Adjust chunksize as needed
            for index, row in chunk.iterrows():
                deviation_id = row['Deviation_id']
                author_name_csv = row['Author_name']
                published_on = row['Published_on']

                # Find the artist in the 'artists' table
                artist = session.query(Artist).filter_by(artist_name=author_name_csv).first()

                if artist:
                    artist_id = artist.id
                    artist_name = artist.artist_name

                    # Check if record already exists in imgs_date
                    existing_date_record = session.query(imgs_date).filter_by(id=deviation_id).first()

                    if existing_date_record:
                        # If record exists, check if artist_name needs to be updated
                        if existing_date_record.artist_name is None or existing_date_record.artist_name != artist_name:
                            existing_date_record.artist_name = artist_name
                            session.add(existing_date_record)
                            total_rows_updated += 1
                            print(f"Updated artist_name for imgs_date record with id: {deviation_id}")
                        else:
                            total_rows_skipped += 1
                            #print(f"Skipping existing imgs_date record with id: {deviation_id} (artist_name already correct)")
                    else:
                        # If record does not exist, create a new one
                        new_image_date = imgs_date(
                            id=deviation_id,
                            artist_id=artist_id,
                            date=published_on,
                            artist_name=artist_name # Populate artist_name here
                        )
                        session.add(new_image_date)
                        total_rows_saved += 1
                        print(f"Saved new imgs_date record: {new_image_date.id}")

                    try:
                        session.commit()  # Commit after each row
                        total_rows_processed += 1
                    except SQLAlchemyError as e:
                        session.rollback()
                        print(f"Error saving/updating record {deviation_id}: {e}")
                        # Handle the error (e.g., log, retry, skip)
                else:
                    print(f"Artist not found in 'artists' table for image date with ID: {deviation_id} (Author_name: {author_name_csv})")

    except Exception as e:
        print(f"An error occurred during incremental save: {e}")
    finally:
        session.close()
        print(f"Incremental save complete. Total processed rows from CSV: {total_rows_processed}. New rows saved: {total_rows_saved}. Existing rows updated: {total_rows_updated}. Existing rows skipped: {total_rows_skipped}.")

# Wrapper function to run the save process
def run_save_imgs_date(engine, image_date_data_path):
    try:
        save_imgs_date_incrementally(engine, image_date_data_path)
    except Exception as e:
        print(f"An error occurred in the saving thread: {e}")

# ... (In your Jupyter notebook cell) ...
if __name__ == "__main__":  # This block will only execute when the script is run directly
    DATABASE_URL =  'sqlite:////mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_main05.db'
    engine = create_engine(DATABASE_URL)  # Create a SQLite database file

    image_date_data_path = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviants_gallData_4_5_6/uniqueDev_gall_SnwBall03_6.2.csv.gz"

    # Create and start the data saving thread
    data_saving_thread = threading.Thread(target=run_save_imgs_date, args=(engine, image_date_data_path))
    data_saving_thread.start()

    print("Image date saving started in a separate thread. You can now interact with other parts of the notebook.")

    # If you need to wait for the saving thread to finish, uncomment the line below:
    # data_saving_thread.join()
    # print("\nImage date saving thread finished.")