import functions_framework
import random, string
import time
import json
import datetime
import pandas as pd
from google.cloud import storage
import os


def output_path_generator(table_name):
    output_bucket='preprocessed_data_12345'
    x = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))
    output_file_name=str(time.time()).split('.')[0]+"_"+x+'.parquet'
    output_path = 'gs://{}/{}/'.format(output_bucket,table_name)
    output_blob_name = os.path.join(output_path,output_file_name)
    return output_blob_name
def leagues_extract(d):
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
    return (leagues_df,leagues_played_df)

@functions_framework.cloud_event
def hello_auditlog(cloudevent):
    # Google Cloud Storage client
    storage_client = storage.Client()
    x = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))
    payload = cloudevent.data.get("protoPayload")
    bucket=cloudevent.data['resource']['labels']['bucket_name']
    #response=-1  # No processing happend
    if payload:
        key=payload.get('resourceName').split('objects')[-1][1:]
        blob = storage_client.bucket(bucket).get_blob(key)
        content = blob.download_as_text()
        d=json.loads(content)
        leagues_df,leagues_played_df=leagues_extract(d)
        leagues_df.to_parquet(output_path_generator('leagues'),index=False )
        leagues_played_df.to_parquet(output_path_generator('leagues-played'),index=False )