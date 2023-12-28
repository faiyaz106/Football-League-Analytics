import awswrangler as wr
import pandas as pd
import json
import urllib.parse
import os
import boto3
s3=boto3.client('s3')

# Temporary hard-coded AWS Settings; i.e. to be set as OS variable in Lambda
os_input_s3_players_preprocessed = os.environ['s3_players_preprocessed']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
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
    players={'id':[],'name':[],'birth':[],'nationality':[],'height_cm':[],'weight_kg':[]}
    if 'results' in d and d['results']==1:
        players['id'].append(d['response'][0]['player']['id'])
        players['name'].append(d['response'][0]['player']['name'])
        players['birth'].append(d['response'][0]['player']['birth']['date'])
        players['nationality'].append(d['response'][0]['player']['nationality'])
        height=d['response'][0]['player']['height']
        if height is not None:
            height=height.strip()
            if height!='':
                height=int(height.split(" ")[0])
            else:
                height=None
        players['height_cm'].append(height)
        weight=d['response'][0]['player']['weight']
        if weight is not None:
            weight=weight.strip()
            if weight!='':
                weight=int(weight.split(" ")[0])
            else:
                weight=None
        players['weight_kg'].append(weight)
    players_df=pd.DataFrame(players)
    # Handling None values
    string_column_set={'name','nationality'}
    date_column_set={'birth'}
    integer_column_set={'id'}
    columns=list(players.keys())
    for column in columns:
        if column in string_column_set:
            players_df[column]=players_df[column].astype('str')
        elif column in date_column_set:
            continue
        elif column in integer_column_set:
            players_df[column]=pd.to_numeric(players_df[column], errors='coerce').astype('Int32')
        else:
            players_df[column]=pd.to_numeric(players_df[column], errors='coerce').astype('float')
    wr_response = wr.s3.to_parquet(
            df=players_df,
            path=os_input_s3_players_preprocessed,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation)
    return wr_response
