import boto3
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from io import BytesIO
 
# AWS S3 credentials
s3_client = boto3.client('s3', aws_access_key_id='****',
                         aws_secret_access_key='****',
                         region_name='eu-north-1') #client to access my S3 bucket
BUCKET_NAME = 'rearc-quest-sam'
BLS_URL = 'https://download.bls.gov/pub/time.series/pr/'
API_url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
FILE_NAME = 'population_data.json'
CSV_FILE_NAME = 'pr.data.0.Current'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def handler(event, context):
    """Entry point for the data scraper Lambda."""
    sync_bls_data()
    sync_population_data()

def get_file_list():
    response = requests.get(BLS_URL, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    files = ['https://download.bls.gov/' + link.get('href') for link in soup.find_all('a')][1:]
    return files

def upload_to_s3(file_url):
    file_name = file_url.split('/')[-1]
    local_path = f'/tmp/{file_name}'
    
    response = requests.get(file_url, headers=headers)
    with open(local_path, 'wb') as f:
        f.write(response.content)
    
    try:
        s3_client.upload_file(local_path, BUCKET_NAME, file_name)
        print(f"{file_name} uploaded to S3.")
    except Exception as e:
        print(f"Failed to upload {file_name}: {e}")

def sync_bls_data():
    files = get_file_list()
    current_files = [obj['Key'] for obj in s3_client.list_objects_v2(Bucket=BUCKET_NAME).get('Contents', [])]

    for file_url in files:
        file_name = file_url.split('/')[-1]
        if file_name not in current_files:
            upload_to_s3(file_url)

def sync_population_data():
    response = requests.get(API_url)
    if response.status_code == 200:
        data = response.json()
        json_data = json.dumps(data['data'], indent=4)
        json_buffer = BytesIO(json_data.encode('utf-8'))
        json_buffer.seek(0)
        try:
            s3_client.upload_fileobj(json_buffer, BUCKET_NAME, FILE_NAME)
            print(f"{FILE_NAME} uploaded successfully to {BUCKET_NAME}.")
        except Exception as e:
            print(f"Failed to upload to S3: {e}")
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
