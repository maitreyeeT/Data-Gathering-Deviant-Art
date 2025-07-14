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

from sqlalchemy import text

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
def save_imgs_date_incrementally(session, image_date_data_path):
    """Saves imgs_date data incrementally, skipping existing records."""
    engine = session.get_bind()
    total_rows_saved = 0

    try:
        for chunk in pd.read_csv(image_date_data_path, chunksize=1000, on_bad_lines='skip'):  # Adjust chunksize as needed
            for index, row in chunk.iterrows():
                artist = session.query(Artist).filter_by(artist_name=row['Author_name']).first()
                if artist:
                    artist_id = artist.id
                    artist_name = artist.artist_name

                    # Check if record already exists
                    existing_date_record = session.query(imgs_date).filter_by(id=row['Deviation_id']).first()
                    if existing_date_record:
                        print(f"Skipping duplicate imgs_date record with id: {row['Deviation_id']}")
                        continue

                    new_image_date = imgs_date(id=row['Deviation_id'], artist_id=artist_id, date=row['Published_on'], artist_name = artist_name)
                    session.add(new_image_date)

                    try:
                        session.commit()  # Commit after each row
                        total_rows_saved += 1
                        print(f"Saved imgs_date record: {new_image_date.id}, Total saved: {total_rows_saved}")
                    except SQLAlchemyError as e:
                        session.rollback()
                        print(f"Error saving record: {e}")
                        # Handle the error (e.g., log, retry, skip)
                else:
                    print(f"Artist not found for image date with ID: {row['Deviation_id']}")
        # Place the print statement BEFORE the exceptions:
        print(f"All data from CSV '{image_date_data_path}' saved to table ''")  
    
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()

# ... (In your Jupyter notebook cell) ...
if __name__ == "__main__":  # This block will only execute when the script is run directly
    DATABASE_URL =  'sqlite:////mnt/hdd/maittewa/deviantArt_DeviantData/deviantArt_main05.db'
    engine = create_engine(DATABASE_URL)  # Create a SQLite database file
    Base = declarative_base()
    Session = sessionmaker(bind=engine)
    session = Session()
    image_date_data_path = "/mnt/hdd/maittewa/deviantArt_DeviantData/deviants_gallData_4_5_6/uniqueDev_gall_SnwBall03_6.2.csv.gz"
    # Assuming 'session' is your SQLAlchemy session object
    try:
        save_imgs_date_incrementally(session, image_date_data_path)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()  # Close the session in the finally block to ensure it's closed even if errors occur