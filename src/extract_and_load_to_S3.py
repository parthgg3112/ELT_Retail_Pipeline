

import pandas as pd
from sqlalchemy import create_engine
import boto3
import os
import time
import json
from botocore.exceptions import ClientError

def get_secret():
    """Fetches credentials from AWS Secrets Manager."""
    secret_name = "project_retail/secrets"
    region_name = "us-east-2" 

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        print("Fetching secrets")
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        print("Secrets fetched successfully.")
        return json.loads(secret)
    except ClientError as e:
        print(f"Error fetching secret: {e}")
        raise e

def extract_and_load_sales():
    """Extracts sales data from Postgres, saves as Parquet, and uploads to S3."""
    start_time = time.time()
    creds = get_secret()
    print(creds)
    
    db_url = f"postgresql://{creds['username']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['dbname']}"
    engine = create_engine(db_url)
    bucket_name = creds['s3_bucket']
    
    tables = ['sales_transactions', 'sales_details']
    s3_client = boto3.client('s3')

    if not os.path.exists('src/data_parquet'):
        os.makedirs('src/data_parquet')

    for table_name in tables:
        print(f"\nExtracting table: {table_name}...")
        df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        print("Printing df")
        
        file_name = f"{table_name}_{int(time.time())}.parquet"
        local_file_path = os.path.join('src/data_parquet', file_name)
        print(local_file_path)
        df.to_parquet(local_file_path, index=False)
        table_domain = table_name.split('_')[0]
        table_type = table_name.split('_')[1]


    #     # Upload to a specific sub-prefix for better organization
        s3_key = f'{table_domain}/{table_type}/{file_name}'
        print(f"Uploading to s3://{bucket_name}/{s3_key}...")
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        print("Upload successful")

    end_time = time.time()
    print(f"\nSales ETL job finished in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    extract_and_load_sales()