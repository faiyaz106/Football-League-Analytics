import awswrangler as wr
import pandas as pd
import json
import urllib.parse
import os
import boto3

s3=boto3.client('s3')

# Temporary hard-coded AWS Settings; i.e. to be set as OS variable in Lambda
os_input_s3_venues_preprocessed = os.environ['s3_venues_preprocessed']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_venues = os.environ['glue_catalog_table_name_venues']
os_input_write_data_operation = os.environ['write_data_operation']
 

def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    venues={'id':[],'name':[],'capacity':[],'city':[],'surface':[],'country':[]}
    try:
        s3_response = s3.get_object(Bucket=bucket,Key=key)
        # Get the Body object in the S3 get_object() response
        s3_object_body = s3_response.get('Body')
        # Read the data in bytes format
        content = s3_object_body.read()
        d = json.loads(content)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
        
    if 'results' in d and d['results']>=1:
        for i in range(0,len(d['response'])):
            id=d['response'][i]['id']
            name=d['response'][i]['name']
            capacity=d['response'][i]['capacity']
            city=d['response'][i]['city']
            surface=d['response'][i]['surface']
            country=d['response'][i]['country']
            venues['id'].append(id)
            venues['name'].append(name)
            venues['capacity'].append(capacity)
            venues['city'].append(city)
            venues['surface'].append(surface)
            venues['country'].append(country)
    venues_df=pd.DataFrame(venues)
    # Handling None values
    string_column_set={'name','city','surface','country'}
    columns=list(venues.keys())
    for column in columns:
        if column in string_column_set:
            venues_df[column]=venues_df[column].astype('str')
        else:
            venues_df[column]=pd.to_numeric(venues_df[column], errors='coerce').astype('Int32')

    response=wr.s3.to_parquet(
            df=venues_df,
            path=os_input_s3_venues_preprocessed,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_venues,
            mode=os_input_write_data_operation)

    return {
        'statusCode': 200,
        'body': response
    }
