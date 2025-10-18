# lambda_function.py

import json
import boto3
import os
from datetime import datetime
import urllib3


s3_client = boto3.client('s3')
http = urllib3.PoolManager()

def lambda_handler(event, context):
    try:
        api_url = os.environ['API_URL']
        bucket_name = os.environ['S3_BUCKET_NAME']
    except KeyError as e:
        print(f"ERROR: Environment variable {e} not set!")
        raise e
        
    s3_prefix = 'products'
    
    print(f"Calling API at: {api_url}")
    
    try:
        response = http.request('GET', api_url)
        
        if response.status != 200:
            print(f"ERROR: API returned status code {response.status} with body: {response.data.decode('utf-8')}")
            raise Exception(f"API request failed with status code {response.status}")
        
        product_data = response.data 
        
        timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H%M%S')
        file_name = f"product_catalog_{timestamp}.json"
        s3_key = f"{s3_prefix}/{file_name}"

        print(f"Writing data to s3://{bucket_name}/{s3_key}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=product_data
        )
        
        print("Successfully wrote product data to S3.")
        return {
            'statusCode': 200,
            'body': json.dumps(f"Successfully ingested {file_name} to S3.")
        }
        
    except Exception as e:
        print(f"An error occurred: {e}")
        raise e