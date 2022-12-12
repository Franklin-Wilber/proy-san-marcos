import json
import os
import helpers

root_path = os.path.dirname(os.path.abspath(__file__))

file_params_pubsub_path = root_path+"/config.json"

file_params_pubsub = open(file_params_pubsub_path)
pubsubObject = json.load(file_params_pubsub)

file_credential_path = root_path+'/'+pubsubObject['file_credential_path']
file_credentials = open(file_credential_path)
credentialsObject = json.load(file_credentials)

project_id = credentialsObject['project_id']

PUB_THREAD_GAS_SEND = pubsubObject['PUB_THREAD_GAS_SEND']
SUB_THREAD_PY_RECEIVE = helpers.getSubscriptionThreadName()

PUB_THREAD_PY_REQUEST = pubsubObject['PUB_THREAD_PY_REQUEST']
SUB_THREAD_GAS_PROCESS = pubsubObject['SUB_THREAD_GAS_PROCESS']

DB_PATH_KOLIBRI = pubsubObject['DB_PATH_KOLIBRI']
DIRECTORY_PATH_FILES_IMPORT = root_path+pubsubObject['DIRECTORY_PATH_FILES_IMPORT']

courses_topic_name = pubsubObject['courses_topic_name']
courses_subscriptor_name = pubsubObject['courses_subscriptor_name']

metadata_topic_name = pubsubObject['PUB_METADA']

courses_subscription_path = 'projects/'+project_id+'/subscriptions/'+courses_subscriptor_name
courses_topic_path = 'projects/'+project_id+'/topics/'+courses_topic_name

metadata_topic_path = 'projects/'+project_id+'/topics/'+metadata_topic_name