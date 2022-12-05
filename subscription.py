import os
from google.cloud import pubsub_v1
import config
import helpers

serial_number = helpers.getSerialNumber()

def create():
    if serial_number == None:
        print("Serial Number no encontrado")
    else:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.file_credential_path

        publisher  = pubsub_v1.PublisherClient()
        subscriber = pubsub_v1.SubscriberClient()

        PROJECT_ID = config.project_id
        PUB_THREAD_PY_REQUEST = config.PUB_THREAD_PY_REQUEST
        SUB_THREAD_PY_RECEIVE = config.SUB_THREAD_PY_RECEIVE

        topic_path = publisher.topic_path(PROJECT_ID,PUB_THREAD_PY_REQUEST)
        subscription_path = subscriber.subscription_path(PROJECT_ID,SUB_THREAD_PY_RECEIVE)
        exists = False
        
        response = publisher.list_topic_subscriptions(request={ "topic": topic_path })
        for subscription_path_item in response:
            if subscription_path_item == subscription_path:
                exists = True

        print(topic_path)
        print(subscription_path)

        if exists == False :
            with subscriber:
                subscription = subscriber.create_subscription(
                    request={ "name": subscription_path, "topic": topic_path }
                )
                print(subscription)
        else:
            print("Subscription exists")

