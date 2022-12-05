import os
from google.cloud import pubsub_v1
import config
import helpers
import datetime
import json
import base64

def publish(subscription,status,data):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.file_credential_path
    PROJECT_ID = config.project_id
    PUB_THREAD_PY_REQUEST = config.PUB_THREAD_PY_REQUEST

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID,PUB_THREAD_PY_REQUEST)
    date = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    data = data.encode('utf-8')
    attributes = {
        'status': status,
        'datetime': date,
        'subscription': subscription,
        'device_id': helpers.getSerialNumber()
    }
    future = publisher.publish(topic_path, data, **attributes)
    print(f'published message id {future.result()}')

