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

import pandas as pd
import boto3

class S3ParquetReader:
    def __init__(self, bucket_name, aws_access_key_id=None, aws_secret_access_key=None):
        self.bucket_name = bucket_name
        self.s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    def read_parquet(self, key):
        response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
        body = response['Body']
        df = pd.read_parquet(body)
        return df
    
    s3_reader = S3ParquetReader(bucket_name='my-bucket', aws_access_key_id='my-access-key', aws_secret_access_key='my-secret-key')
df = s3_reader.read_parquet('path/to/my/file.parquet')

#-----------------------
import pyarrow.parquet as pq
import pandas as pd
import boto3

class S3ParquetReader:
    def __init__(self, s3_bucket, s3_prefix):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_client = boto3.client('s3')

    def get_object_keys(self):
        response = self.s3_client.list_objects_v2(Bucket=self.s3_bucket, Prefix=self.s3_prefix)
        return [obj['Key'] for obj in response['Contents']]

    def read_parquet_files(self):
        s3_keys = self.get_object_keys()
        df_list = []
        for key in s3_keys:
            obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=key)
            table = pq.read_table(obj['Body'])
            df = table.to_pandas()
            df_list.append(df)
        return pd.concat(df_list, ignore_index=True)
    
  #-------
reader = S3ParquetReader('my-s3-bucket', 'my-s3-prefix')
df = reader.read_parquet_files()  

