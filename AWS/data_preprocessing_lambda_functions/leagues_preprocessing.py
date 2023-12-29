import awswrangler as wr
import pandas as pd
import json
import urllib.parse
import os
import boto3
import datetime
s3=boto3.client('s3')

# Temporary hard-coded AWS Settings; i.e. to be set as OS variable in Lambda
os_input_s3_leagues_preprocessed = os.environ['s3_players_preprocessed_leagues']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_leagues = os.environ['glue_catalog_table_name_leagues']
os_input_s3_leagues_preprocessed_leages_played = os.environ['s3_players_preprocessed_leagues_played']
os_input_glue_catalog_leagues_played = os.environ['glue_catalog_table_name_leagues_played']
os_input_write_data_operation = os.environ['write_data_operation']
 

def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(bucket,key)
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
    leagues={'id':[],'name':[],'country':[],'type':[]}
    leagues_played={'league_id':[],'start':[],'end':[],'season':[]}
    format = '%Y-%m-%d'
    if 'results' in d and d['results']>=1:
        for i in range(0,len(d['response'])):
            id=d['response'][i]['league']['id']
            name=d['response'][i]['league']['name']
            country=d['response'][i]['country']['name']
            typ=d['response'][i]['league']['type']
            leagues['id'].append(id)
            leagues['name'].append(name)
            leagues['country'].append(country)
            leagues['type'].append(typ)
            for j in range(0,len(d['response'][i]['seasons'])):
                start=d['response'][i]['seasons'][j]['start']
                end=d['response'][i]['seasons'][j]['end']
                start=datetime.datetime.strptime(start, format)
                end=datetime.datetime.strptime(end, format)
                season=d['response'][i]['seasons'][j]['year']
                leagues_played['league_id'].append(id)
                leagues_played['start'].append(start.date()) 
                leagues_played['end'].append(end.date())
                leagues_played['season'].append(season)
    leagues_df=pd.DataFrame(leagues)
    leagues_played_df=pd.DataFrame(leagues_played)
    # Handling None values
    string_column_set={'name','country','type'}
    columns=list(leagues.keys())
    for column in columns:
        if column in string_column_set:
            leagues_df[column]=leagues_df[column].astype('str')
        else:
            leagues_df[column]=pd.to_numeric(leagues_df[column], errors='coerce').astype('Int32')
    response_1=wr.s3.to_parquet(
            df=leagues_df,
            path=os_input_s3_leagues_preprocessed,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_leagues,
            mode=os_input_write_data_operation)
    response_2=wr.s3.to_parquet(
            df=leagues_played_df,
            path=os_input_s3_leagues_preprocessed_leages_played,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_leagues_played,
            mode=os_input_write_data_operation)
    return (response_1,response_2)