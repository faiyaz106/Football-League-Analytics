import functions_framework
from datetime import date, timedelta
import datetime
import requests
from google.cloud import storage
import os
import json 
import time

api_key="cffedefeife76dp1dfwefiewfj4834fdjdfhefe"  # put actual api key ..Not shared due to security reason.
api_host="api-football-v1.p.rapidapi.com"
url="https://api-football-v1.p.rapidapi.com/v3/fixtures"
#api_key=os.environ['api_key']
#api_host=os.environ['api_host']
#url=os.environ['api_url']
#max_request=os.environ['api_max_request']
def fixtures_data_extraction(api_key,api_host,url,fixture_ids):
    """
    api_key: API Key 
    api_host: API Host
    url: API url
    fixture_id: List of Maximum 20 ids
    """
    text=str(fixture_ids[0])
    for i in fixture_ids[1:]:
        text+='-'+str(i)
    querystring = {"ids":text}
    headers = {"X-RapidAPI-Key": api_key,"X-RapidAPI-Host": api_host}
    response = requests.request("GET", url, headers=headers, params=querystring)
    return response

@functions_framework.http
def hello_http(request):
    # Google Cloud Storage client
    storage_client = storage.Client()
    # Json tracking file path
    tracking_file_bucket='raw_data_12345'
    key='tracking_files/fixtures_tracking.json'
    blob = storage_client.bucket(tracking_file_bucket).get_blob(key)
    content = blob.download_as_text()
    # Process the data
    fixtures_tracking=json.loads(content)
    fixture_list=fixtures_tracking['fixtures_id']
    # Fixtures storage
    fixtures_bucket='hello-bucket-12345'
    count=0
    while count<10:
      fixture_ids=fixture_list[:20]
      response=fixtures_data_extraction(api_key,api_host,url,fixture_ids)
      file_name='fixtures/{}_{}_fixtures.json'.format(fixture_ids[0],fixture_ids[-1])
      output_blob=storage_client.bucket(fixtures_bucket).blob(file_name)
      output_blob.upload_from_string(data=json.dumps(response.json()))
      fixtures_tracking['fixtures_id_data_pulled'].extend(fixture_ids)
      fixture_list=fixture_list[20:]
      fixtures_tracking['fixtures_id']=fixture_list
      blob.upload_from_string(data=json.dumps(fixtures_tracking))
      count=count+1
      time.sleep(5)
  
    return {'success':200}









