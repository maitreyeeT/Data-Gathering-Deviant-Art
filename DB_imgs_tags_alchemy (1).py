import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, BigInteger, Boolean, func, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import re, ast
import numpy as np
from sqlalchemy import UniqueConstraint, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
import threading # Import the threading module


Base = declarative_base()

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
    imgs_dscrpt = relationship("imgs_dscrpt", back_populates="artist")  # Establish relationship from Artist to images description
    imgs_tags = relationship("imgs_tags", back_populates="artist")  # Add this line



#Images description class
class imgs_dscrpt(Base):  
    __tablename__ = 'imgs_dscrpt'
    id = Column(String, primary_key=True)
    artist_id = Column(Integer, ForeignKey('artists.id'))  # Link to Artist table
    artist_name = Column(String)
    description = Column(String)

    artist = relationship("Artist", order_by=artist_id, back_populates="imgs_dscrpt")  # Define the relationship
    
    # Add relationship for tags:
    tags = relationship("imgs_tags", back_populates="image_description")  # Specify back_populates

class imgs_tags(Base):  
    __tablename__ = 'imgs_tags'
    tag_id = Column(Integer, primary_key=True, autoincrement=True)
    image_id = Column(String, ForeignKey('imgs_dscrpt.id'))  # Foreign key to imgs_dscrpt
    artist_id = Column(Integer, ForeignKey('artists.id')) 
    artist_name = Column(String)
    tags = Column(String)

    # Relationships (if needed)
    image_description = relationship("imgs_dscrpt", back_populates="tags")  # Relationship to imgs_dscrpt
    artist = relationship("Artist", back_populates="imgs_tags")

def process_tags(tags_string, image_id, artist_id, artist_name, session):
    try:
        tags_list = ast.literal_eval(tags_string)
        for tag in tags_list:
            if pd.isnull(tag) or tag is None:
                continue
            else:
                # tag_id is auto-incremented, no need to provide a value here
                new_image_tag = imgs_tags(
                    image_id=image_id,
                    artist_id=artist_id,
                    artist_name=artist_name,
                    tags=tag.strip()  # Store each tag individually
                )
                session.add(new_image_tag)
    except (SyntaxError, ValueError):
        print(f"Error parsing tags for image ID: {image_id}")


#tag_id INTEGER image_id VARCHAR artist_id INTEGER artist_name VARCHAR tags VARCHAR
def save_imgs_tags_incrementally(engine, image_tags_data_path):
    """Saves imgs_dscrpt data incrementally, row by row."""
    Session = sessionmaker(bind=engine) # Create a Session factory
    session = Session() # Create a session from the factory
    total_rows_saved = 0
    total_rows_skipped = 0


    try:
        # Create tables if they don't exist
        Base.metadata.create_all(engine)

        for chunk in pd.read_csv(image_tags_data_path, chunksize=1000, on_bad_lines='skip'):
            for index, row in chunk.iterrows():
                artist = session.query(Artist).filter_by(artist_name=row['Author_Name']).first()
                if artist:
                    artist_id = artist.id
                    artist_name = artist.artist_name

                    # Check if record already exists (using image_id and tag).
                    # This is a more robust check for uniqueness of individual tags per image.
                    # Assuming that Devtn_Id corresponds to image_id and tag_name to a single tag string in the CSV.
                    # If tag_name in CSV is a list string, process_tags will handle breaking it down.
                    # The check below assumes tag_name in CSV is a single tag string or the whole list string will be checked as one tag.
                    # A better approach for list strings is to check existence within process_tags after splitting.
                    # For now, retaining the original check structure but noting its limitation.

                    # If tag_name in CSV is a list string, the check below will look for the whole list string as a tag.
                    # If tag_name in CSV is a single tag string per row, this check is appropriate.
                    existing_record = session.query(imgs_tags).filter_by(image_id=row['Devtn_Id'], tags=row['tag_name']).first()

                    if existing_record:
                        total_rows_skipped += 1
                        print(f"Skipping duplicate imgs_tags record with id: {row['Devtn_Id']} and tag: {row['tag_name']}")
                        continue

                    # Call process_tags to handle tag creation and insertion
                    # This function adds tags to the session, but doesn't commit.
                    process_tags(row['tag_name'], row['Devtn_Id'], artist_id, artist_name, session)


                    try:
                        session.commit()  # Commit after processing all tags for a row (or per tag if process_tags committed)
                        total_rows_saved += 1
                        # --- This print statement is adjusted to reflect imgs_tags ---
                        # print(f"Saved imgs_dscrpt record: {new_image_dscrpt.id}, Total saved: {total_rows_saved}")
                        # print(f"Saved imgs_tags records for image: {row['Devtn_Id']}, Total saved: {total_rows_saved}") # This might print per row in CSV, adjust if process_tags adds multiple
                    except SQLAlchemyError as e:
                        session.rollback()
                        print(f"Error saving record: {e}")
                        # Handle the error (e.g., log, retry, skip)

                else:
                    print(f"Artist not found for image with ID: {row['Devtn_Id']}")
        # Place the print statement inside the try block, after the loop
        print(f"All data from CSV '{image_tags_data_path}' saved to table 'imgs_tags'")  # Changed to imgs_tags


    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()
        print(f"Incremental save complete. New rows saved: {total_rows_saved}. Existing rows skipped: {total_rows_skipped}.")


# Wrapper function to run the save process in a thread
def run_save_imgs_tags(engine, image_tags_data_path):
    try:
        save_imgs_tags_incrementally(engine, image_tags_data_path)
    except Exception as e:
        print(f"An error occurred in the saving thread for imgs_tags: {e}")


# ... (In your main function or script) ...

if __name__ == "__main__":  # This block will only execute when the script is run directly
    DATABASE_URL =  'sqlite:////mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_main05.db'
    engine = create_engine(DATABASE_URL)  # Create a SQLite database file

    image_dscrpt_data_path = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_metaDataSnwBall/uniqueDev_metaData_SnwBall_02.csv.gz"

    # Create and start the data saving thread for imgs_tags
    data_saving_thread_tags = threading.Thread(target=run_save_imgs_tags, args=(engine, image_dscrpt_data_path))
    data_saving_thread_tags.start()

    print("Image tags saving started in a separate thread. You can now interact with other parts of the notebook.")

    # If you need to wait for the saving thread to finish, uncomment the line below:
    # data_saving_thread_tags.join()
    # print("\nImage tags saving thread finished.")