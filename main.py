from marketorestpython.client import MarketoClient
import pandas as pd
import inspect
from datetime import datetime, timedelta, date
import time

today = date.today()
one_year_ago = datetime.today() - timedelta(days=365)
one_month_ago = datetime.today() - timedelta(days=31)
one_week_ago = datetime.today() - timedelta(days=7)

# Settings

key_fields = ['Id','Last Name','Company Name','Email Address','Product of Interest','APEM Product Series','Updated At','Country','Inferred Country','Industry','Person Score','Created At','DB Score','Person Source','Last Interesting Moment Date','Last Interesting Moment Desc','Last Interesting Moment Source','Last Interesting Moment Type']

munchkin_id = "599-EUJ-018" # fill in Munchkin ID, typical format 000-AAA-000
client_id = "c9784b2f-c6e1-40e8-b185-36f74492b56c" # enter Client ID from Admin > LaunchPoint > View Details
client_secret= "80BG4unAYowH0XvM00UQtlqpxSBnrIqk" # enter Client ID and Secret from Admin > LaunchPoint > View Details
api_limit=None
max_retry_time=None
requests_timeout=(3.0, 40.0)

#Creating instance
mc = MarketoClient(munchkin_id, client_id, client_secret, api_limit, max_retry_time, requests_timeout=requests_timeout)


#Testing system
#downloaded_3Ds = mc.execute(method='get_smart_list_by_id', id=50827, return_full_result=True)
#new_export_job_details = mc.execute(method='create_activities_export_job', fields=key_fields, filters={'createdAt': {'endAt': str(today), 'startAt': str(one_month_ago)}})
#
#Getting activity types to csv
activity_types = mc.execute(method='get_activity_types')
activity_list = []
description_list = []
for i in range(0, len(activity_types)):
    activity_list.append(activity_types[i]['name'])
for i in range(0, len(activity_types)):
    description_list.append(activity_types[i])

#df = pd.DataFrame(list(zip(activity_list, description_list)),
#               columns =['name','description'])

#df.to_csv('marketo_activities.csv')

#Get activities
#activities = mc.execute(method='get_lead_activities', activityTypeIds=['2'], nextPageToken=None, sinceDatetime=one_month_ago,
#                        untilDatetime=str(today), batchSize=None, listId=None, leadIds=[5045892, 11472589, 57453, 11405600])


#smart_list = mc.execute(method='get_smart_list_by_id', id=53533, return_full_result=True)



apem_3D_downloads = 3305

#new_export_job_details = mc.execute(method='create_activities_export_job', fields=['leadId'],filters={'createdAt': {'endAt': str(today), 'startAt': str(one_month_ago)}})
#job_id = new_export_job_details[0]['exportId']
#enqueue_job = mc.execute(method='enqueue_activities_export_job', job_id=job_id)

#GET ALL EXPORT JOBS
def export_jobs():
    return mc.execute(method='get_activities_export_jobs_list')

def get_list_by_id(list_id):
    response = mc.execute(method='get_multiple_leads_by_list_id', listId=list_id,
               fields=['id', 'email', 'firstName', 'lastName', 'company', 'postalCode'], batchSize=None)
    print(response)
    print (type(response))

get_list_by_id(apem_3D_downloads)

#CREATE NEW EXPORT JOB
def create_export_job():
    new_export_job_details = mc.execute(method='create_activities_export_job', fields=['leadId'],filters={'staticListId': apem_3D_downloads,'createdAt': {'endAt': str(today), 'startAt': str(one_month_ago)}})

    job_id = new_export_job_details[0]['exportId']
    enqueued_job_details = mc.execute(method='enqueue_activities_export_job', job_id=job_id)

    export_job_status = mc.execute(method='get_activities_export_job_status', job_id=job_id)
    print(export_job_status)

    while export_job_status[0]['status'] != 'Completed':
        export_job_status = mc.execute(method='get_activities_export_job_status', job_id=job_id)
        print(export_job_status)
        time.sleep(30)

    with mc.execute(method='get_activities_export_job_file', job_id=job_id, stream=True) as r:
        with open('export_job_test.csv', 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
