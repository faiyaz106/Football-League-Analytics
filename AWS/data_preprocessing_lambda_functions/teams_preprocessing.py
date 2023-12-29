import awswrangler as wr
import pandas as pd
import json
import urllib.parse
import os
import boto3
s3=boto3.client('s3')

# Temporary hard-coded AWS Settings; i.e. to be set as OS variable in Lambda
os_input_s3_teams_preprocessed = os.environ['s3_teams_preprocessed']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_teams = os.environ['glue_catalog_table_name_teams']
os_input_write_data_operation = os.environ['write_data_operation']
 

def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
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
    teams={'id':[],'name':[],'founded':[],'national':[],'venue_id':[],'country':[]}
    if 'results' in d and d['results']>=1:
        id=d['response'][0]['team']['id']
        name=d['response'][0]['team']['name']
        founded=d['response'][0]['team']['founded'] 

        national=d['response'][0]['team']['national']
        venue_id=d['response'][0]['venue']['id']
        country=d['response'][0]['team']['country']
        teams['id'].append(id)
        teams['name'].append(name)
        teams['founded'].append(founded)
        teams['national'].append(national)
        teams['venue_id'].append(venue_id)
        teams['country'].append(country)
    teams_df=pd.DataFrame(teams)
    teams_df['founded']=pd.to_numeric(teams_df.founded, errors='coerce').astype('Int32')
    # Handling None values
    string_column_set={'name','country'}
    boolean_column_set={'national'}
    columns=list(teams.keys())
    for column in columns:
        if column in string_column_set:
            teams_df[column]=teams_df[column].astype('str')
        elif column in boolean_column_set:
            teams_df[column]=teams_df[column].astype('boolean')
        else:
            teams_df[column]=pd.to_numeric(teams_df[column], errors='coerce').astype('Int32')
    response=wr.s3.to_parquet(
            df=teams_df,
            path=os_input_s3_teams_preprocessed,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_teams,
            mode=os_input_write_data_operation)
    return { 'status':200, 'body':response }