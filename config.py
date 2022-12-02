import json
import os

root_path = os.path.dirname(os.path.abspath(__file__))

file_params_pubsub_path = root_path+"/config.json"

file_params_pubsub = open(file_params_pubsub_path)
pubsubObject = json.load(file_params_pubsub)

file_credential_path = root_path+'/'+pubsubObject['file_credential_path']
file_credentials = open(file_credential_path)
credentialsObject = json.load(file_credentials)

project_id = credentialsObject['project_id']
thread_topic_id = pubsubObject['thread_topic_id']
thread_topic_path = 'projects/'+project_id+'/topics/'+thread_topic_id

courses_topic_name = pubsubObject['courses_topic_name']
courses_subscriptor_name = pubsubObject['courses_subscriptor_name']

courses_subscription_path = 'projects/'+project_id+'/subscriptions/'+courses_subscriptor_name
courses_topic_path = 'projects/'+project_id+'/topics/'+courses_topic_name