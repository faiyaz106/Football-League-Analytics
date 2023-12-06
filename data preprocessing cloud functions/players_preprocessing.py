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

def players_extract(d):
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
    return players_df

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
        players_df=players_extract(d)
        players_df.to_parquet(output_path_generator('players'),index=False )