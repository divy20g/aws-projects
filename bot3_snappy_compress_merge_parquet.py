import boto3
import io
import os
import pyarrow.parquet as pq
import pyarrow as pa
import snappy

def lambda_handler(event, context):
    # Set up S3 client
    s3 = boto3.client('s3')
    
    # Set up input/output S3 bucket and folder paths
    input_bucket = 'input-bucket'
    output_bucket = 'output-bucket'
    input_folder = 'input-folder/'
    output_folder = 'output-folder/'
    
    # List all Parquet files in input folder
    response = s3.list_objects_v2(Bucket=input_bucket, Prefix=input_folder)
    files = [file['Key'] for file in response['Contents'] if file['Key'].endswith('.parquet')]
    
    # Read in and merge Parquet files
    table = None
    for file in files:
        obj = s3.get_object(Bucket=input_bucket, Key=file)
        data = io.BytesIO(obj['Body'].read())
        table_chunk = pq.read_table(data)
        if table is None:
            table = table_chunk
        else:
            table = pa.concat_tables([table, table_chunk])
    
    # Compress merged table with Snappy and write to output folder
    compressed_data = snappy.compress(table.serialize())
    output_file = output_folder + 'merged.parquet.snappy'
    s3.put_object(Bucket=output_bucket, Key=output_file, Body=compressed_data)
    
    return {
        'statusCode': 200,
        'body': 'Merged and compressed {} Parquet files'.format(len(files))
    }
