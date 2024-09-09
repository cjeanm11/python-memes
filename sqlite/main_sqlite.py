import requests, logging, os, json, argparse, sys
from typing import Optional
from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import pandas as pd

Base = declarative_base()
session: Session
class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    userId = Column(Integer)
    title = Column(String)
    body = Column(String)

def request_data(url: str) -> Optional[requests.Response]:
    try:
        response = requests.get(url)
        response.raise_for_status()
        logging.debug(f"status_code: {response.status_code}, body: {response.json}")
        return response
    except requests.HTTPError as e:
        logging.error(f'HTTP request error: {e}')
    except requests.RequestException as e:
        logging.error(f'Request exception: {e}')
    except Exception as e:
        logging.error(f'Unknown error: {e}')

def process_response(response: requests.Response) -> pd.DataFrame:
    data = response.json()
    if not data:
        logging.info("json is empty")
        return pd.DataFrame()
    try:
        df = pd.DataFrame(data)
        logging.info(f"columns: {df.columns}")
        return df
    except Exception as e:
        logging.error(f"Processing error: {e}")
    return pd.DataFrame()
        
def handle_response(response: Optional[requests.Response]) -> Optional[pd.DataFrame]:
    match response:
        case requests.Response() as res:
            return process_response(res)
        case _:
            logging.error("failed request")

def load(session, df: pd.DataFrame) -> None:
    try:
        with session.begin():
            for _, row in df.iterrows():
                existing_user = session.query(User).filter_by(id=row['id']).first()
                if existing_user is None:
                    new_user = User(
                        id=row['id'],
                        userId=row['userId'],
                        title=row['title'],
                        body=row['body']
                    )
                    session.add(new_user)
    except Exception as e:
        print(f"An error occurred: {e}")
        session.rollback()  
        
def store_data(df: pd.DataFrame) -> bool:
    try:
        query = text(f"select * from users;")
        load(session, df)
        with session.begin():
            result = session.execute(query)
            print("RESULT")
            for row in result:
                logging.debug(f"userId: {row.userId}, id: {row.id}")
        return True
    finally:
        session.close()
    return False
    
def init(debug: bool = False):
    global session
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    engine = create_engine('sqlite:///example.db', echo=True)
    Base.metadata.create_all(engine)
    SessionFactory = sessionmaker(bind=engine)
    session = SessionFactory()    
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A script for loading data with SQLAlchemy.")
    parser.add_argument('--debug', action='store_true', help="Enable debug logging")
    args = parser.parse_args()
    init(debug=args.debug)
    response = request_data(url='https://jsonplaceholder.typicode.com/posts')
    returned_df = handle_response(response)
    match returned_df:
        case pd.DataFrame() as res:
            if store_data(res):
                logging.info("Sucessfully stored data.")
        case _:
            sys.exit(-1)
            


