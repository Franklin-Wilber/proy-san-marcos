import os
from google.cloud import pubsub_v1
import config
import helpers
import datetime
import json
import base64

def publish(status,action,data):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.file_credential_path
    PROJECT_ID = config.project_id
    PUB_THREAD_PY_REQUEST = config.PUB_THREAD_PY_REQUEST

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID,PUB_THREAD_PY_REQUEST)
    date = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    data = data.encode('utf-8')
    attributes = {
        'status': status,
        'action': action,
        'datetime': date,
        'device_id': helpers.getSerialNumber()
    }
    future = publisher.publish(topic_path, data, **attributes)
    print(f'published message id {future.result()}')


def sub(subscription):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.file_credential_path
    subscriber = pubsub_v1.SubscriberClient()

    def callback(message):
        print(f'Received message: {message}')
        print(f'data: {message.data}')
        if message.attributes:
            print("Attributes:")
            for key in message.attributes:
                value = message.attributes.get(key)
                print(f"{key}: {value}")
        message.ack()           
        print("************ ************ ************ ************ ************")

    subscription_path = subscriber.subscription_path(config.project_id,subscription)
    print(subscription_path)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f'Listening for messages on {subscription_path}')

    with subscriber:
        try:
            streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()