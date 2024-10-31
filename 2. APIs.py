import requests
import json
import boto3
from io import BytesIO


# Define the API URL
API_url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"

s3_client = boto3.client('s3', aws_access_key_id='***',
                         aws_secret_access_key='***',
                         region_name='eu-north-1') #client to access my S3 bucket
BUCKET_NAME = 'rearc-quest-sam'
FILE_NAME = 'population_data.json'

# Make a GET request to the API
response = requests.get(API_url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    data = response.json()  # Parse the response as JSON
    # Convert the data to a JSON byte stream for uploading

    json_data = json.dumps(data['data'], indent=4) # Convert the 'data' section of the JSON response into a JSON-formatted string
    json_buffer = BytesIO(json_data.encode('utf-8')) # Encode the JSON string into a byte stream, preparing it for S3 upload
    json_buffer.seek(0)  # Reset the buffer's pointer to the beginning so all data is available for upload
    try:
        s3_client.upload_fileobj(json_buffer, BUCKET_NAME, FILE_NAME) # Upload the data as a file object to S3 with the specified bucket and file name
        print(f"{FILE_NAME} uploaded successfully to {BUCKET_NAME}.")
    except Exception as e:
        print(f"Failed to upload to S3: {e}")
    
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
