import pyarrow.parquet as pq
import s3fs

def lambda_handler(event, context):
    s3 = s3fs.S3FileSystem()

    # Set the bucket name and prefix for input files
    bucket_name = 'my-bucket'
    input_prefix = 'input/'

    # Set the bucket name and prefix for the output file
    output_prefix = 'output/merged.snappy.parquet'

    # Get a list of input file paths
    input_paths = [f"s3://{bucket_name}/{key}" for key in s3.ls(input_prefix)]

    # Read input files into a list of Arrow tables
    tables = [pq.read_table(path) for path in input_paths]

    # Merge the tables
    merged_table = pq.concat_tables(tables)

    # Write the merged table to an output file on S3
    output_path = f"s3://{bucket_name}/{output_prefix}"
    pq.write_table(merged_table, output_path, compression='snappy')

    return {
        'statusCode': 200,
        'body': f'Merged and compressed {len(input_paths)} files into {output_path}'
    }
