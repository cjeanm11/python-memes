import requests, logging, os, json
import pandas as pd
from typing import Optional
data = []

def request_data(url: str) -> Optional[requests.Response]:
    try:
        response = requests.get(url)
        logging.debug(f"status_code: {response.status_code}, body: {response.json}")
        response.raise_for_status()
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
    df = pd.DataFrame(data)
    try:
        logging.info(f"columns: {df.columns}")
        filtered_df: pd.DataFrame = df[df['userId'] == 1]
        return filtered_df
    except Exception as e:
        logging.error(f"Processing error: {e}")
    return pd.DataFrame()
        
def handle_response(response: Optional[requests.Response]) -> Optional[pd.DataFrame]:
    match response:
        case requests.Response() as res:
            return process_response(res)
        case _:
            logging.error("failed request")

def store_data(df: pd.DataFrame):
    json_output = df.to_json('./out_3.json',orient='records', indent=4)

if __name__ == "__main__":
    response = request_data(url='https://jsonplaceholder.typicode.com/posts')
    returned_df = handle_response(response)
    match returned_df:
        case pd.DataFrame() as res:
            store_data(res)
        case _:
            os._exit(-1)