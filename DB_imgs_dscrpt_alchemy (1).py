from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, BigInteger, Boolean, func, MetaData, Table
from sqlalchemy.orm import sessionmaker, relationship
import re, ast
import numpy as np
from sqlalchemy import UniqueConstraint, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
import pandas as pd

DATABASE_URL =  'sqlite:////mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_main05.db'
engine = create_engine(DATABASE_URL)  # Create a SQLite database file
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

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


#Images description class
class imgs_dscrpt(Base):  
    __tablename__ = 'imgs_dscrpt'
    id = Column(String, primary_key=True)
    artist_id = Column(Integer, ForeignKey('artists.id'))  # Link to Artist table
    artist_name = Column(String)
    description = Column(String)

    artist = relationship("Artist", order_by=artist_id, back_populates="imgs_dscrpt")  # Define the relationship




#function to clean the description in metadata for description table
def clean_description(description):
    if pd.isnull(description) or description is None:  # Check for NaN or None
        return ""  # Return empty string for NaN values
    elif isinstance(description, str):  # Check if it's a string
        description = re.sub(r"<br\s*/?>", "\n", description)
        description = re.sub(r"<[^>]+>", "", description)
        description = re.sub(r"[^a-zA-Z0-9 ]", "", description)
        return description
    else:
        #print(f"Unexpected data type for description: {type(description)}")  # Optional: print for debugging
        try: 
            return str(description) # Attempt to convert to string, but still remove HTML tags
        except Exception as e:
            print(f"Error converting description to string: {e}. Returning an empty string instead.")
            return "" 


def save_imgs_dscrpt_incrementally(session, image_dscrpt_data_path):
    """Saves imgs_dscrpt data incrementally, row by row."""
    engine = session.get_bind()  # Get the engine from the session
    total_rows_saved = 0

    try:
        for chunk in pd.read_csv(image_dscrpt_data_path, chunksize=1000, on_bad_lines='skip'):  # Adjust chunksize as needed
            for index, row in chunk.iterrows():
                artist = session.query(Artist).filter_by(artist_name=row['Author_Name']).first()
                if artist:
                    artist_id = artist.id
                    artist_name = artist.artist_name
                    cleaned_description = clean_description(row['Devtn_Descp'])

                    # Check if record already exists 
                    existing_record = session.query(imgs_dscrpt).filter_by(id=row['Devtn_Id']).first()
                    if existing_record:
                        print(f"Skipping duplicate imgs_dscrpt record with id: {row['Devtn_Id']}")
                        continue 

                    new_image_dscrpt = imgs_dscrpt(id=row['Devtn_Id'], artist_id=artist_id,
                                                   artist_name=artist_name, description=cleaned_description)
                    session.add(new_image_dscrpt)

                    try:
                        session.commit()  # Commit after each row
                        total_rows_saved += 1
                        print(f"Saved imgs_dscrpt record: {new_image_dscrpt.id}, Total saved: {total_rows_saved}")
                    except SQLAlchemyError as e:
                        session.rollback()
                        print(f"Error saving record: {e}")
                        # Handle the error (e.g., log, retry, skip)

                else:
                    print(f"Artist not found for image with ID: {row['Devtn_Id']}")
        # Place the print statement BEFORE the exceptions:
        print(f"All data from CSV '{image_dscrpt_data_path}' saved to table 'imgs_dscrpt'")  
    
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()

# ... (In your main function or script) ...

if __name__ == "__main__":  # This block will only execute when the script is run directly

    image_dscrpt_data_path = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_snwBall_fin/deviants_metaDataSnwBall/uniqueDev_metaData_SnwBall_02.csv.gz"

    try:
        save_imgs_dscrpt_incrementally(session, image_dscrpt_data_path)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()  # Close the session in the finally block to ensure it's closed even if errors occur