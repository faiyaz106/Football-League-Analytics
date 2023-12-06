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

def teams_extract(d):
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
    return teams_df

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
        teams_df=teams_extract(d)
        teams_df.to_parquet(output_path_generator('teams'),index=False )