import os
from google.cloud import pubsub_v1
import config
import helpers
import datetime
import json
import base64

def publish(data):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.file_credential_path
    publisher = pubsub_v1.PublisherClient()
    date = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    data = data.encode('utf-8')
    attributes = {
        'time': date,
        'device_id': helpers.getSerialNumber()
    }
    future = publisher.publish(config.thread_topic_path, data, **attributes)
    print(f'published message id {future.result()}')

