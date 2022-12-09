import os
from google.cloud import pubsub_v1
import config
import helpers
import datetime
import json
import base64
import dao.MonitorProcessDAO
import dao.MonitorProcessItemDAO
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

        python_monitor_process_id = attributes.get('python_monitor_process_id')
        python_monitor_process_item_id = attributes.get('python_monitor_process_item_id')

        print(python_monitor_process_id)
        print(python_monitor_process_item_id)
        
        status = attributes.get('status')
        
        resultData = json.loads(data,encoding='utf-8')

        if( device_id == helpers.getSerialNumber() ):
            if(action == 'sync-people'):
                list_data = resultData['list_data']
                print('data ==> ')
                # message.ack()
                print('***********************')
                print(python_monitor_process_id)
                monitorProcess = None
                if(python_monitor_process_id == None):
                    python_monitor_process_id = 0
                else:
                    python_monitor_process_id = python_monitor_process_id

                if( int(python_monitor_process_id) > 0):
                    monitorProcess = dao.MonitorProcessDAO.find(python_monitor_process_id)
                else:
                    params = ('python3 execute_cmd.py --action sync-people',
                            'sync-people', 'GAS',gas_monitor_process_id,0,0, 'PENDING')
                    result = dao.MonitorProcessDAO.insert(params)
                    if (result == True):
                        monitorProcess = dao.MonitorProcessDAO.lastInserted()
                        print('monitorProcess => Proceso creado')
                    else:
                        print('monitorProcess => No se pudo crear el proceso')
                
                print(monitorProcess)
                print('***********************')
                if(monitorProcess):
                    
                    paramsItem = ( python_monitor_process_id,part_num_total_items,gas_monitor_process_id,gas_monitor_process_item_id,'PENDING' )
                    resultItemState = dao.MonitorProcessItemDAO.insert(paramsItem)
                    if(resultItemState):
                        params = ( num_total_items,gas_monitor_process_id,gas_monitor_process_item_id,'PENDING',python_monitor_process_id)
                        dao.MonitorProcessDAO.changeState(params)

                        for item in list_data :            
                            student_id = item['id']
                            campus_name = item['data']['campus_name']
                            grade_name = item['data']['grade_name']
                            level_name = item['data']['level_name']
                            section_name = item['data']['section_name']
                            study_group_code = item['data']['study_group_code']
                            updated_at = item['data']['updated_at']
                            print( student_id + " " + campus_name + " " + grade_name + " " + level_name + " " + section_name + " " + study_group_code + " " + updated_at )
                    
            elif(action == 'sync-courses'):
                # message.ack()
                print("************ ************ ************ ************ ************")
            elif(action == 'sync-studyGroup'):
                # message.ack()
                print("************ ************ ************ ************ ************")
            message.ack()
        else:
            print("No hay data por procesar")

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