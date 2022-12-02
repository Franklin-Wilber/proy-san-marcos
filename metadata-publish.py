import os
from google.cloud import pubsub_v1
import config
import datetime
import json
import base64

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.file_credential_path

publisher = pubsub_v1.PublisherClient()

data_json2 = [
    {"canal":"23","tarea":"18","fecha":"30/11/2022"},
    {"atributo":"23","tarea":"18","fecha":"30/11/2022"},
    {"tareas":"23","curso":"Algebra","fecha":"30/11/2022"}
]

data_json = '{"tareas":"23","curso":"Algebra","fecha":"31/11/2022"}'

print(type(data_json))
print(type(data_json2))
date_time = datetime.datetime.now()
date = date_time.strftime("%d/%m/%Y %H:%M:%S")

data = data_json
data = data.encode('utf-8')
attributes = {
    'time': date,
    'id_device':'rasp01'
}
future = publisher.publish(config.topic_path, data, **attributes)
print(f'published message id {future.result()}')

