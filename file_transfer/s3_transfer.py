import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from typing import Optional
from dataclasses import dataclass

@dataclass(frozen=True)
class S3Config:
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    region_name: Optional[str] = None

class S3Client:
    
    def __init__(self, config: S3Config):
           self.s3 = boto3.client(
               's3',
               aws_access_key_id=config.aws_access_key_id,
               aws_secret_access_key=config.aws_secret_access_key,
               region_name=config.region_name
           ) 

    def upload_parquet_to_s3(self, bucket_name: str, file_path: str, object_name: str) -> None:
        """Upload a file to S3 bucket."""
        try:
            self.s3.upload_file(file_path, bucket_name, object_name)
            print("Upload Successful")
        except FileNotFoundError:
            print("The file was not found")
        except NoCredentialsError:
            print("Credentials not available")
        except PartialCredentialsError:
            print("Incomplete credentials provided")
        except Exception as e:
            print(f"An error occurred: {e}")

    def set_lifecycle_policy(self, bucket_name: str) -> None:
        """Set a lifecycle policy for the S3 bucket."""
        lifecycle_policy = {
            'Rules': [
                {
                    'ID': 'Move to Glacier after 365 days',
                    'Filter': {'Prefix': ''},
                    'Status': 'Enabled',
                    'Transitions': [
                        {
                            'Days': 365,
                            'StorageClass': 'GLACIER'
                        }
                    ],
                    'Expiration': {
                        'Days': 3650
                    }
                }
            ]
        }
        try:
            self.s3.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle_policy)
            print("Lifecycle policy set successfully")
        except NoCredentialsError:
            print("Credentials not available")
        except PartialCredentialsError:
            print("Incomplete credentials provided")
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    config = S3Config(
           aws_access_key_id=None,
           aws_secret_access_key=None, 
           region_name=None  
       )
    
    s3_client = S3Client(config)
    
    bucket_name = 'my-bucket'
    file_path = './data.parquet'
    object_name = 'folder/data.parquet'
    
    s3_client.upload_parquet_to_s3(bucket_name, file_path, object_name)
    s3_client.set_lifecycle_policy(bucket_name)