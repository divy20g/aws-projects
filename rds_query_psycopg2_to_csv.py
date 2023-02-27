#working script
import psycopg2
import boto3
import pandas as pd
#from pandas import Series, DataFrame
import logging
import os
import io
#import awswrangler as wr

bucket_name = 'database-query-results-stage'
s3_object_name = 'source-file/df_from_rds.csv'

s3_client = boto3.client('s3')

def lambda_handler(event,context):
    conn_string = "host = 'database-2.chuvhg3vzbxm.us-east-1.rds.amazonaws.com' dbname = 'postgres' user= 'postgres' password = 'postgresadmin'"
    tablename = 'public.orders'

    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    print ("Connected to the Database")

    cursor.execute("SELECT * FROM " + tablename +" limit 100;")
    myresult = cursor.fetchall()
    item_list = []
    for i in myresult:
        item = {'order_id':i[0],
        'order_date':i[1],
        'quantity' :i[2],
        'notes' :i[3]}
        item_list.append(item)
        concat = str(i[0]) + str(',') + str(i[1]) + str(',') + str(i[2]) + str(',') + str(i[3])
    # print (concat)
    df = pd.DataFrame(data=item_list,columns=['order_id','order_date','quantity','notes'])
    df.head()
    #print (df.head())

    with io.StringIO() as csv_buffer:
        df.to_csv(csv_buffer, index=False)

        response = s3_client.put_object(
            Bucket=bucket_name, Key=s3_object_name, Body=csv_buffer.getvalue()
        )

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")
