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

        project_id = config.project_id
        topic_id = config.thread_topic_id
        subscription_id = helpers.getSubscriptionThreadName()

        print(subscription_id)

        subscription_path = subscriber.subscription_path(project_id,subscription_id)
        exists = False

        topic_path = publisher.topic_path(project_id,topic_id)
        response = publisher.list_topic_subscriptions(request={ "topic": topic_path })
        for subscription_path_item in response:
            if subscription_path_item == subscription_path:
                exists = True

        if exists == False :
            with subscriber:
                subscription = subscriber.create_subscription(
                    request={ "name": subscription_path, "topic": topic_path }
                )
                print(subscription)
        else:
            print("Subscription exists")

