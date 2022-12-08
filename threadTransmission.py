import os
from google.cloud import pubsub_v1
import config
import helpers
import datetime
import json
import base64
import dao.MonitorProcessDAO
import codecs

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
        attributes = message.attributes
        data = message.data
        data = codecs.decode(data,'utf-8')
        # print( attributes.get('status') )
        action = attributes.get('action')
        device_id = attributes.get('device_id')
        school_id = attributes.get('school_id')
        datetime = attributes.get('datetime')        
        subscription = attributes.get('subscription')
        num_parts = attributes.get('num_parts')
        num_total_items = attributes.get('num_total_items')
        part_num_total_items = attributes.get('part_num_total_items')

        gas_monitor_process_id = attributes.get('gas_monitor_process_id')
        gas_monitor_process_item_id = attributes.get('gas_monitor_process_item_id')
        status = attributes.get('status')
        
        resultData = json.loads(data,encoding='utf-8')
        # num_parts = resultData['num_parts']
        # num_total_items = resultData['num_total_items']
        # part_num_total_items = resultData['part_num_total_items']
        list_data = resultData['list_data']

        print('data ==> ')
        # print(resultData)
        # message.ack()
        # return False
        # print(device_id)
        # print(school_id)
        # print(process_python_id)
        if( device_id == helpers.getSerialNumber() ):
            # params = ( num_items,'PENDING',python_monitor_process_id)
            # dao.MonitorProcessDAO.changeStateEvent(params)
            for item in list_data :            
                student_id = item['id']
                campus_name = item['data']['campus_name']
                grade_name = item['data']['grade_name']
                level_name = item['data']['level_name']
                section_name = item['data']['section_name']
                study_group_code = item['data']['study_group_code']
                updated_at = item['data']['updated_at']
                print( student_id + " " + campus_name + " " + grade_name + " " + level_name + " " + section_name + " " + study_group_code + " " + updated_at )

        # message.ack()
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