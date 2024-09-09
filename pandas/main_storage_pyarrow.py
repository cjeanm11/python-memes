import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import os
import uuid
import shutil

"""
Write new data to a partitioned Parquet file.
"""
def create_partition_parquet(new_data: pd.DataFrame, partition_dir: str):
    os.makedirs(partition_dir, exist_ok=True)
    unique_id = uuid.uuid4().hex
    temp_file = os.path.join(partition_dir, f'{unique_id}.parquet')
    new_table = pa.Table.from_pandas(new_data)
    pq.write_table(new_table, temp_file)
    print(f'New partition created: {temp_file}')

"""
Combine all partition files into a single Parquet file.
"""
def combine_partitions_to_parquet(file_path: str, partition_dir: str):
    all_files = [os.path.join(partition_dir, f) for f in os.listdir(partition_dir) if f.endswith('.parquet')]
    
    if not all_files:
        print('No partition files to combine.')
        return
    
    tables = [pq.read_table(f) for f in all_files]
    combined_table = pa.concat_tables(tables)
    
    pq.write_table(combined_table, file_path)
    print(f'Combined data written to {file_path}')
    
    for file in all_files:
        os.remove(file)

"""
Delete a directory and its contents.
"""
def delete_directory(directory: str):
    if os.path.exists(directory):
        try:
            shutil.rmtree(directory)
            print(f'Directory {directory} deleted successfully.')
        except PermissionError as e:
            print(f'Permission error: {e}')
        except FileNotFoundError as e:
            print(f'File not found error: {e}')
        except Exception as e:
            print(f'Error: {e}')
    else:
        print(f'Directory {directory} does not exist.')


"""
Read a Parquet file into a Pandas DataFrame.

Args:
    file_path (str): Path to the Parquet file.
    
Returns:
    pd.DataFrame: DataFrame containing the data from the Parquet file.
"""
def read_parquet(file_path: str) -> pd.DataFrame:
    table = pq.read_table(file_path)
    df = table.to_pandas()
    return df

"""
Example function to demonstrate usage of partitioned Parquet files.
"""
def pyarrow_example():
    new_data1 = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    new_data2 = pd.DataFrame({'A': [5, 6], 'B': [7, 8]})

    create_partition_parquet(new_data1, './partitions')
    create_partition_parquet(new_data2, './partitions')

    combine_partitions_to_parquet('./data.parquet', 'partitions')
    df: pd.DataFrame = read_parquet('./data.parquet')
    print(df)
    

if __name__ == '__main__':
    # Uncomment to delete the partitions directory
    # delete_directory('./partitions')
    pyarrow_example()