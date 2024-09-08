import re, csv, os
import logging
from typing import List, Optional, Tuple, Union

    
class Task:
    
    def read(self, file_name: str) -> Union[List[str], Exception]:
        datas = []
        try:
            with open('./in.txt','r') as fs:
                datas = [line.rstrip('\n') for line in fs] 
        except FileNotFoundError as e:
            logging.error(f"File not found: {file_name}")
            return e
        except IOError as e:
            logging.error(f"IOError occurred while reading the file: {file_name}")
            return e
        return datas
    
    def process(self, datas: Optional[List[str]]) -> List[List[str]]:
        if datas is None:
            return []
        email_pattern = re.compile(r'([A-Za-z0-9]+)@([A-Za-z0-9]+)\.([A-Za-z0-9]+)')
        processes_data = []
        incorrect_data = []
        for data in datas:
            matcher = re.search(email_pattern, data)
            if matcher is not None:
                p1, p2, p3 = matcher.groups()
                processes_data.append([p1,p2,p3])
            else:
                incorrect_data.append(data)
        return processes_data
        
    def load(self, file_name: str, processes_data: List[List[str]]) -> None:
        processes_data.insert(0, ['p1','p2','p3'])
        try:
            with open(file_name, mode='w', newline='') as file:
                writer = csv.writer(file, delimiter=';')
                writer.writerows(processes_data)
        except FileNotFoundError as e:
            logging.error(f"File not found: {file_name}. Error: {e}")
        except IOError as e:
            logging.error(f"IOError occurred while reading the file: {file_name}. Error: {e}")
        
            
def run_task():
    task = Task()
    match task.read('./in.txt'):
        case Exception() as e:
            logging.error(e)
            return 0
        case list() as datas:
            processes_data: List[List[str]] = task.process(datas)
            task.load('./out.csv', processes_data)
            
    
if __name__ == '__main__':
    run_task()

    