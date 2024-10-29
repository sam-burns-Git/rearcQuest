import boto3
import requests
from bs4 import BeautifulSoup

# AWS S3 credentials
s3_client = boto3.client('s3', aws_access_key_id='AKIAUW4RBBFE5L3N4TNZ',
                         aws_secret_access_key='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',
                         region_name='eu-north-1') #client to access my S3 bucket
BUCKET_NAME = 'rearc-quest-sam' #
BLS_URL = 'https://download.bls.gov/pub/time.series/pr/' #Base URL
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7; rv:109.0) Gecko/20100101 Firefox/117.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://download.bls.gov/',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin'
}


def get_file_list():
    # Fetch list of files from BLS website
    response = requests.get(BLS_URL, headers=headers) #Access the BLS URL using the user agent to navigate around 403 error
    soup = BeautifulSoup(response.text, 'html.parser') #Scrape the data using beautiful Soup
    files = ['https://download.bls.gov/' + link.get('href') for link in soup.find_all('a')] #add all the links on the site to an array
    files = files[1:] #remove the parent directory
    return files

def upload_to_s3(file_url):
    # Download a file and upload to S3.
    file_name = file_url.split('/')[-1] #get the file name from the link to it
    local_path = f'/tmp/{file_name}' #get the local file path
    
    # Download file
    response = requests.get(file_url, headers=headers) # Access the files url 
    with open(local_path, 'wb') as f:
        f.write(response.content)
    
    # Upload to S3
    try:
        s3_client.upload_file(local_path, BUCKET_NAME, file_name) #
        print(f"{file_name} uploaded to S3.")
    except Exception as e:
        print(f"Failed to upload {file_name}: {e}")

def sync_bls_data():
    """Sync files from BLS to S3."""
    files = get_file_list()
    
    # Get current files in S3
    current_files = [obj['Key'] for obj in s3_client.list_objects_v2(Bucket=BUCKET_NAME).get('Contents', [])]

    for file_url in files:
        file_name = file_url.split('/')[-1]
        if file_name not in current_files:
            upload_to_s3(file_url) #uplaod any files that are present in BLS URL and not in S3 bucket

if __name__ == '__main__':
    sync_bls_data()




