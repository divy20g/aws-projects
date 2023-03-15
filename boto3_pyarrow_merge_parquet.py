import boto3
import pyarrow.parquet as pq
import pyarrow as pa

s3 = boto3.resource('s3')

def lambda_handler(event, context):
    # Replace with your S3 bucket name and directory path
    bucket_name = 'my-s3-bucket'
    directory_path = 'my-directory-path'

    bucket = s3.Bucket(bucket_name)
    files = []
    for obj in bucket.objects.filter(Prefix=directory_path):
        if obj.key.endswith('.parquet'):
            files.append('s3://{}/{}'.format(bucket_name, obj.key))

    if not files:
        print('No Parquet files found in S3')
        return

    # Read in the Parquet files using PyArrow
    tables = [pq.ParquetDataset(file).read() for file in files]

    # Merge the tables using PyArrow
    merged_table = pa.concat_tables(tables)

    # Replace with your desired output file name and directory path
    output_file_name = 'merged.parquet'
    output_directory_path = 'my-output-directory-path'

    # Write the merged table back to S3 as a Parquet file
    output_file = 's3://{}/{}/{}'.format(bucket_name, output_directory_path, output_file_name)
    pq.write_table(merged_table, output_file)

    print('Successfully merged Parquet files and saved to S3')
