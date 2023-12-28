import functions_framework
from datetime import date, timedelta
import datetime
import requests
from google.cloud import storage
import os
import json 

api_key="cfdfefeefedifiejieijfeijfffffferfeiif"   # use your own api key ( Here i put random api key due to privacy reason)
api_host="api-football-v1.p.rapidapi.com"
url="https://api-football-v1.p.rapidapi.com/v3/fixtures"
max_request=2
#api_key=os.environ['api_key']
#api_host=os.environ['api_host']
#url=os.environ['api_url']
#max_request=os.environ['api_max_request']

def extract_features_ids(api_key,api_host,date,url):
    querystring = {"date":date}
    headers = {"X-RapidAPI-Key": api_key,"X-RapidAPI-Host": api_host}
    response = requests.get(url, headers=headers, params=querystring)
    return response

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

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
    date_str=fixtures_tracking["last_updated"] # The date - 29 Dec 2017
    format_str='%Y-%m-%d' # The format
    last_updated=datetime.datetime.strptime(date_str, format_str).date()
    start_date=last_updated+timedelta(1)
    end_date=min(date.today(),start_date+timedelta(max_request))
    for date_ in daterange(start_date,end_date):
        response=extract_features_ids(api_key,api_host,date_,url)
        res_dict=response.json()
        for i,v in enumerate(res_dict['response']):
            fix_id=v['fixture']['id']
            fixtures_tracking['fixtures_id'].append(fix_id)
        fixtures_tracking['date_covered'].append(str(date_))
        fixtures_tracking['last_updated']=str(date_)
    blob.upload_from_string(data=json.dumps(fixtures_tracking))
    return {'success':200}









