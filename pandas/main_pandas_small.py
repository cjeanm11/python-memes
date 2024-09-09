import pandas as pd
import time
import logging

def extract(file_path: str, chunksize: int):
    for chunk in pd.read_csv(file_path, chunksize=chunksize):
        yield chunk

def process_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    start_time = time.time()
    chunk_cleaned = chunk.dropna()
    chunk_cleaned['Age'] = chunk_cleaned['Age'].astype(int)
    chunk_filtered = chunk_cleaned[chunk_cleaned['Age'] > 18]
    end_time = time.time()
    logging.info(f"Processed chunk in {end_time - start_time:.2f} seconds")
    return chunk_filtered

def load(df: pd.DataFrame, output_file_path: str):
    df.to_csv(output_file_path, index=False)

def process_small_csv(file_path: str, output_file_path: str, chunksize: int = 100000):
    start_time = time.time()
    chunk_list = []
    
    for chunk_number, chunk in enumerate(extract(file_path, chunksize), start=1):
        logging.info(f"Processing chunk {chunk_number}")
        processed_chunk = process_chunk(chunk)
        chunk_list.append(processed_chunk)
    
    final_df = pd.concat(chunk_list)
    load(final_df, output_file_path)
    
    end_time = time.time()
    logging.info(f"Processed large CSV file in {end_time - start_time:.3f} seconds")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    input_file = 'small_dataset.csv'
    output_file = 'processed_dataset_pandas.csv'
    process_small_csv(input_file, output_file)