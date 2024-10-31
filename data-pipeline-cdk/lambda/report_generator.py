import boto3
import pandas as pd
import json
from io import BytesIO

# AWS S3 client
s3_client = boto3.client('s3', aws_access_key_id='****',
                         aws_secret_access_key='****',
                         region_name='eu-north-1') #client to access my S3 bucket
BUCKET_NAME = 'rearc-quest-sam'
JSON_FILE_NAME = 'population_data.json'
CSV_FILE_NAME = 'pr.data.0.Current'

def handler(event, context):
    """Entry point for the report generation Lambda."""
    analyse_data()

def load_json_from_s3():
    try:
        json_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=JSON_FILE_NAME)
        json_data = json_obj['Body'].read().decode('utf-8')
        json_dict = json.loads(json_data)
        df_json = pd.DataFrame(json_dict)
        return df_json
    except Exception as e:
        print(f"Error loading JSON: {e}")
        return None

def load_csv_from_s3():
    try:
        csv_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=CSV_FILE_NAME)
        csv_data = csv_obj['Body']
        df_csv = pd.read_csv(BytesIO(csv_data.read()), delimiter='\t')
        return df_csv
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return None

def analyse_data():
    df_population = load_json_from_s3()
    df_current = load_csv_from_s3()

    if df_population is None or df_current is None:
        print("Data loading failed. Exiting analysis.")
        return

    # Population analysis
    df_population['Year'] = pd.to_numeric(df_population['Year'])
    df_13to18 = df_population[(df_population['Year'] >= 2013) & (df_population['Year'] <= 2018)]
    popStdDev = df_13to18['Population'].std()
    popMean = df_13to18['Population'].mean()
    print(f"Population StdDev: {popStdDev}, Mean: {popMean}")

    # BLS data analysis
    df_current.columns = df_current.columns.str.strip()
    current_SeriesYear = df_current.groupby(['series_id', 'year']).sum()
    id_max = current_SeriesYear.groupby('series_id')['value'].idxmax()
    result = current_SeriesYear.loc[id_max].reset_index()
    result = result.drop(columns=['period', 'footnote_codes'])
    print(result)

    # Join data
    joinedTable = df_current[['series_id','year','period','value']].merge(
        df_population[['Year','Population']],
        right_on='Year',
        left_on='year',
        how='inner'
    ).query("series_id == 'PRS30006032' and period == 'Q01'").drop(columns="Year")

    print(joinedTable)
