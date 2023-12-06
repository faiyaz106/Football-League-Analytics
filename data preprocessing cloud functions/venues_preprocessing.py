import functions_framework
import random, string
import time
import json
import urllib.parse
import pandas as pd
from google.cloud import storage
import os

# Venues Preprocessing function
def venues_preprocessing(d):
    """
    d: dictionary input 
    """
    venues={'id':[],'name':[],'capacity':[],'city':[],'surface':[],'country':[]}
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
    return venues_df

# CloudEvent function to be triggered by an Eventarc Cloud Audit Logging trigger
# Note: this is NOT designed for second-party (Cloud Audit Logs -> Pub/Sub) triggers!
@functions_framework.cloud_event
def hello_auditlog(cloudevent):
    # Google Cloud Storage client
    storage_client = storage.Client()
    output_bucket='preprocessed_data_12345'
    gcp_output_path = 'gs://{}/venues/'.format(output_bucket)
    x = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))
    output_file_name='venues_'+str(time.time()).split('.')[0]+"_"+x+'.parquet'
    payload = cloudevent.data.get("protoPayload")
    bucket=cloudevent.data['resource']['labels']['bucket_name']
    #response=-1  # No processing happend
    if payload:
        key=payload.get('resourceName').split('objects')[-1][1:]
        output_blob_name = os.path.join(gcp_output_path,output_file_name)
        blob = storage_client.bucket(bucket).get_blob(key)
        content = blob.download_as_text()
        # Process the data
        d=json.loads(content)
        df=venues_preprocessing(d)
        df.to_parquet(output_blob_name,index=False )
        response=len(df)
    return {'statusCode':200,'body':response}