import requests, logging, os, json
import dask.dataframe as dd
import pandas as pd
from typing import Optional
import time

def extract(file_path: str) -> dd.DataFrame:
    start_time = time.time()
    df = dd.read_csv(file_path, blocksize='100MB')
    end_time = time.time()
    logging.info(f"Extraction completed in {end_time - start_time:.2f} seconds")
    return df

def transform(df: dd.DataFrame) -> dd.DataFrame:
    start_time = time.time()
    df_cleaned = df.dropna()
    df_cleaned['Age'] = df_cleaned['Age'].astype(int)
    df_filtered = df_cleaned[df_cleaned['Age'] > 18]
    end_time = time.time()
    logging.info(f"Transformation completed in {end_time - start_time:.2f} seconds")
    return df_filtered

def load(df: dd.DataFrame, output_file_path: str) -> None:
    start_time = time.time()
    df.to_csv(output_file_path, index=False, single_file=True)
    end_time = time.time()
    logging.info(f"Loading completed in {end_time - start_time:.2f} seconds")

def etl_pipeline(input_file_path: str, output_file_path: str):
    start_time = time.time()
    
    df: dd.DataFrame = extract(input_file_path)
    logging.info(f"DataFrame loaded with {len(df)} partitions")
    
    df_transformed = transform(df)
    logging.info(f"Transformed DataFrame with {len(df_transformed)} partitions")
    
    load(df_transformed, output_file_path)
    
    end_time = time.time()
    logging.info(f"ETL pipeline completed in {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    input_file = 'small_dataset.csv'
    output_file = 'cleaned_dataset.csv'
    etl_pipeline(input_file, output_file)
