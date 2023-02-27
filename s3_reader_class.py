import boto3
import pandas as pd

class S3Reader:
    def __init__(self, access_key, secret_key, bucket_name):
        self.s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        self.bucket_name = bucket_name
    
    def read_csv_to_dataframe(self, file_key, delimiter=','):
        obj = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
        df = pd.read_csv(obj['Body'], delimiter=delimiter)
        return df

access_key = 'your_access_key_here'
secret_key = 'your_secret_key_here'
bucket_name = 'your_bucket_name_here'

s3_reader = S3Reader(access_key, secret_key, bucket_name)


file_key = 'path/to/your/file.csv'
delimiter = ','

df = s3_reader.read_csv_to_dataframe(file_key, delimiter)


