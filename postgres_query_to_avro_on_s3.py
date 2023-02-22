import psycopg2
import io
import fastavro
import boto3

def lambda_handler(event, context):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host="<your-database-host>",
        database="<your-database-name>",
        user="<your-database-username>",
        password="<your-database-password>",
        port="<your-database-port>"
    )

    # Execute the SQL query
    cursor = conn.cursor()
    cursor.execute("<your-sql-query>")
    rows = cursor.fetchall()

    # Convert the query results to an Avro binary format
    avro_schema = {
        "type": "record",
        "name": "YourRecordName",
        "fields": [
            {"name": "Field1", "type": ["null", "string"]},
            {"name": "Field2", "type": ["null", "int"]},
            # add more fields as needed
        ]
    }

    buffer = io.BytesIO()
    fastavro.writer(buffer, avro_schema, rows)

    # Upload the Avro file to S3
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('<your-bucket-name>')
    bucket.put_object(Key='<your-object-key>.avro', Body=buffer.getvalue())

    return "Success"
import os
import pandas_gbq
import boto3

# Set up credentials and permissions
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/credentials.json"
s3 = boto3.client("s3")

# Query BigQuery table and convert to DataFrame
query = "SELECT * FROM my_dataset.my_table"
df = pandas_gbq.read_gbq(query, project_id="my_project")

# Process DataFrame and write to CSV
df_filtered = df[df["some_column"] > 0]
csv_data = df_filtered.to_csv(index=False)

# Upload CSV to S3 bucket
bucket_name = "my_bucket"
key = "path/to/my_file.csv"
s3.put_object(Body=csv_data, Bucket=bucket_name, Key=key)
