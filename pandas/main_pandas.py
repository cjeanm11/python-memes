import re
import pandas as pd
import logging
from typing import List, Optional, Union

class Task:
        
    def read(self, file_name: str) -> Union[pd.DataFrame, Exception]:
        try:
            df = pd.read_csv(file_name)
            return df
        except FileNotFoundError as e:
            logging.error(f"File not found: {file_name}. Error: {e}")
            return e
        except IOError as e:
            logging.error(f"IOError occurred while reading the file: {file_name}. Error: {e}")
            return e

    def process(self, df: Optional[pd.DataFrame]) -> pd.DataFrame:
            if df is None or df.empty:
                return pd.DataFrame()
            
            email_pattern = re.compile(r'([A-Za-z0-9]+)@([A-Za-z0-9]+)\.([A-Za-z0-9]+)')
            processes_data: List[List[str]] = []
            
            for data in df['data']:
                matcher = re.search(email_pattern, data)
                if matcher is not None:
                    p1, p2, p3 = matcher.groups()
                    processes_data.append([p1, p2, p3])
            return pd.DataFrame(processes_data,columns=['p1','p2','p3'])        
        
    def load(self, file_name: str, df: pd.DataFrame) -> None:
        try:
            df.to_csv(file_name, header=['p1','p2','p3'], index=False, sep=',')  # Use comma as separator for CSV
        except FileNotFoundError as e:
            logging.error(f"File not found: {file_name}. Error: {e}")
        except IOError as e:
            logging.error(f"IOError occurred while saving the file: {file_name}. Error: {e}")

def run_task():
    task = Task()
    result = task.read('./in.csv')
    
    match result:
        case Exception() as e:
            logging.error(e)
            return 0
        case pd.DataFrame() as df:
            processes_df = task.process(df)
            task.load('./out_2.csv', processes_df)
            return 1
        case _:
            logging.error("Unexpected result type.")
            return 0
    
if __name__ == '__main__':
    run_task()