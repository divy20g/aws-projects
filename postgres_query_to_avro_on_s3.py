import psycopg2
import io
import fastavro
import boto3

def lambda_handler(event, context):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host="database-2.chuvhg3vzbxm.us-east-1.rds.amazonaws.com",
        database="postgres",
        user="postgres",
        password="postgresadmin",
        port="5432"
    )
    tablename = 'public.orders'
    # Execute the SQL query
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM " + tablename +" limit 100;")
    rows = cursor.fetchall()
    print(rows)

    # Convert the query results to an Avro binary format
    avro_schema = {
        "type": "record",
        "name": "orders",
        "fields": [
            {"name": "order_id", "type": ["null", "int"]},
            {"name": "order_date", "type": ["null", "int"]},
            {"name": "quantity", "type": ["null", "int"]},
            {"name": "notes", "type": ["null", "string"]}
            # add more fields as needed
        ]
    }

    buffer = io.BytesIO()
    fastavro.writer(buffer, avro_schema, rows)

    # Upload the Avro file to S3
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('database-query-results-stage')
    bucket.put_object(Key='myfile_orders.avro', Body=buffer.getvalue())

    return "Success"
