import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, BigInteger, Boolean, func, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import re, ast
import numpy as np
from sqlalchemy import UniqueConstraint, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text


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

#imgs_dscrpt.tags = relationship("imgs_tags", order_by=imgs_tags.tag_id, back_populates="image_description")  # Back-reference in imgs_dscrpt


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
def save_imgs_tags_incrementally(session, image_tags_data_path):
    """Saves imgs_dscrpt data incrementally, row by row."""
    engine = session.get_bind()  # Get the engine from the session
    total_rows_saved = 0

    try:
        for chunk in pd.read_csv(image_tags_data_path, chunksize=1000, on_bad_lines='skip'):
            for index, row in chunk.iterrows():
                artist = session.query(Artist).filter_by(artist_name=row['Author_Name']).first()
                if artist:
                    artist_id = artist.id
                    artist_name = artist.artist_name

                    # Call process_tags to handle tag creation and insertion
                    process_tags(row['tag_name'], row['Devtn_Id'], artist_id, artist_name, session) 

                    # Check if record already exists (using image_id)
                    existing_record = session.query(imgs_tags).filter_by(image_id=row['Devtn_Id']).first()
                    if existing_record:
                        print(f"Skipping duplicate imgs_tags record with id: {row['Devtn_Id']}")
                        continue 

                    # --- The new_image_tags object is created within process_tags ---
                    # new_image_tags = imgs_tags(id=)  <-- This line is removed

                    # --- session.add(new_image_dscrpt) is incorrect; removed ---

                    try:
                        session.commit()  # Commit after each row
                        total_rows_saved += 1
                        # --- This print statement is adjusted to reflect imgs_tags ---
                        # print(f"Saved imgs_dscrpt record: {new_image_dscrpt.id}, Total saved: {total_rows_saved}")
                        print(f"Saved imgs_tags records for image: {row['Devtn_Id']}, Total saved: {total_rows_saved}") 
                    except SQLAlchemyError as e:
                        session.rollback()
                        print(f"Error saving record: {e}")
                        # Handle the error (e.g., log, retry, skip)

                else:
                    print(f"Artist not found for image with ID: {row['Devtn_Id']}")
        # Place the print statement BEFORE the exceptions:
        print(f"All data from CSV '{image_tags_data_path}' saved to table 'imgs_tags'")  # Changed to imgs_tags
    
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()

# ... (In your main function or script) ...

if __name__ == "__main__":  # This block will only execute when the script is run directly
    DATABASE_URL =  'sqlite:////mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_main05.db'
    engine = create_engine(DATABASE_URL)  # Create a SQLite database file
    Session = sessionmaker(bind=engine)
    session = Session()

    image_tags_data_path = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_metaDataSnwBall/uniqueDev_metaData_SnwBall_02.csv.gz"

    try:
        save_imgs_tags_incrementally(session, image_tags_data_path)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()  # Close the session in the finally block to ensure it's closed even if errors occur
