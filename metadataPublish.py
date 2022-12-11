import os
from google.cloud import pubsub_v1
import config
import datetime
import json
import helpers
import dao.DBManager
import configSanMarcos
import pandas as pd
import sqlite3
import sys
from sqlite3 import Error


def listCourse(id_device):
    conn = None
    list_users = []
    try:
        conn = dao.DBManager.connect()
        query = "SELECT id,name,kind FROM kolibriauth_collection where kind='classroom'"
        #query = "select teacher.id,teacher.full_name, strftime('%Y/%m/%d',SUBSTRING(l.date_created, 1, 10)) as anio, strftime('%m',SUBSTRING(l.date_created, 1, 10)) as mes, count(teacher.id) as numero_lesson from lessons_lesson as l INNER JOIN kolibriauth_facilityuser as teacher ON l.created_by_id=teacher.id where anio='2022/12/05' Group By anio,mes, teacher.id ORDER BY anio,mes"
        result = conn.execute(query)     
        
        for row in result:
            #list_users.append({ "id": row[0],"name": row[1],"kind": row[2]})
            print(row[0]+"\t"+row[1]+"\t"+row[2])
            list_users.append({"device_id":id_device,"course_id":row[0],"course_name":row[1]})
        return list_users
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def listDataLesson(id_device,val1,val2):
    conn = None
    list_item = []
    try:
        conn = dao.DBManager.connect()
        query = "select teacher.id,teacher.full_name, strftime('%Y/%m/%d',SUBSTRING(l.date_created, 1, 10)) as anio, strftime('%m',SUBSTRING(l.date_created, 1, 10)) as mes, count(teacher.id) as numero_lesson from lessons_lesson as l INNER JOIN kolibriauth_facilityuser as teacher ON l.created_by_id=teacher.id where anio>='"+val1+"' and anio<='"+val2+"' Group By anio,teacher.id ORDER BY anio"
        result = conn.execute(query)     
        
        for row in result:
            print(row[0]+"\t"+row[1]+"\t"+row[2]+"\t"+str(row[4]))
            #list_users.append({"device_id":id_device,"course_id":row[0],"course_name":row[1]})
            list_item.append({ "device_id": id_device,"user_id": row[0],"full_name": row[1],"num_lesson": str(row[4]),"start_date": val1,"end_date": val2})
         
        return list_item
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def frecuencyChannel(id_device,val1,val2):
    conn = None
    list_item = []
    list_lesson_lesson_id = []
    list_lesson_lesson_name = []
    list_lesson_channel_id = []
    list_lesson_channel_name = []
    list_cant_channel = []
    try:
        conn = dao.DBManager.connect()
        for x in range(6):
            query = "SELECT l.id as lesson_id, l.title, JSON_EXTRACT(l.resources,'$["+str(x)+"].channel_id') AS canal_id,c.name as nombre_canal, strftime('%Y/%m/%d',SUBSTRING(l.date_created, 1, 10)) as anio FROM lessons_lesson as l INNER join content_channelmetadata as c ON canal_id=c.id where anio>='"+val1+"' and anio<='"+val2+"' and canal_id IS NOT NULL"
            result = conn.execute(query)   
            for row in result:
                #print(row[0]+"\t"+row[1]+"\t"+row[2]+"\t"+str(row[3]))
                #list_item.append({ "lesson_id": row[0],"name": row[1],"channel_id": row[2],"channel_name": row[3] })   
                list_lesson_lesson_id.append(row[0])
                list_lesson_lesson_name.append(row[1])
                list_lesson_channel_id.append(row[2])
                list_lesson_channel_name.append(row[3])

        df1 = pd.DataFrame({
                'lesson_id': list_lesson_lesson_id,
                'lesson_name': list_lesson_lesson_name,
                'channel_id': list_lesson_channel_id,
                'channel_name': list_lesson_channel_name,
                'cantidad': '1'
            })
        df2 = df1.pivot_table(index = ["channel_id","channel_name"],  values = "cantidad", aggfunc = "count")
        index = df2.index.tolist()
        cantidad = df2.values.tolist()
        for row in range(len(index)):
            print(index[row][0]+"\t"+index[row][1]+"\t"+str(cantidad[row][0] ))
            list_cant_channel.append({ "device_id": id_device,"channel_id":index[row][0],"channel_name":index[row][1],"cantidad": str(cantidad[row][0]),"start_date": val1,"end_date": val2})

        print("*******************")
        print(list_cant_channel)
        return list_cant_channel
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def frecuencyLessonPerTeacher(id_device,val1,val2):
    conn = None
    list_item = []
    try:
        conn = dao.DBManager.connect()
        query = "select teacher.id,teacher.full_name, strftime('%Y/%m/%d',SUBSTRING(l.date_created, 1, 10)) as anio, count(teacher.id) as numero_lesson from lessons_lesson as l INNER JOIN kolibriauth_facilityuser as teacher ON l.created_by_id=teacher.id where anio>='"+val1+"' and anio<='"+val2+"' Group By anio, teacher.id ORDER BY anio"
        result = conn.execute(query)     
        
        for row in result:
            print(row[0]+"\t"+row[1]+"\t"+row[2]+"\t"+str( row[3]))
            list_item.append({ "device_id": id_device,"teacher_id": row[0],"teacher_name": row[1],"anio": row[2],"cantidad":str( row[3]),"start_date": val1,"end_date": val2 })
         
        return list_item
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def node(id_device,val1,val2):
    conn = None
    list_item = []
    try:
        conn = dao.DBManager.connect()
        query = "select id,title,channel_id,description from content_contentnode limit(3000)"
        result = conn.execute(query)     
        
        for row in result:
            print(row[0]+"\t"+row[1]+"\t"+row[2]+"\t"+ row[3])
            list_item.append({ "device_id": id_device,"node_id": row[0],"title": row[1],"channel_id": row[2],"description": ''})
        
        return list_item
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

def publishCourse(option,val1,val2):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.file_credential_path

    publisher = pubsub_v1.PublisherClient()


    date_time = datetime.datetime.now()
    date = date_time.strftime("%Y/%m/%d")
    id_device = helpers.getSerialNumber()
    print(f'Device id {id_device}')

    attributes = {}

    list_item = []
    if option == 'course':
        attributes = {
            'table': 'course',
        }
        list_item = listCourse(id_device)
    elif option == 'lesson':
        attributes = {
            'table': 'lesson',
        }
        list_item = listDataLesson(id_device,val1,val2)
    elif option == 'channel':
        attributes = {
            'table': 'channel',
        }
        list_item = frecuencyChannel(id_device,val1,val2)
    elif option == 'teacher':
        attributes = {
            'table': 'teacher',
        }
        list_item = frecuencyLessonPerTeacher(id_device,val1,val2)

    elif option == 'node':
        attributes = {
            'table': 'node',
        }
        list_item = node(id_device,val1,val2)
    #data_json = []
    #for row in list_item:        
        #print(row['id']+"\t"+row['name']+"\t"+row['kind']+"\t"+row['kind1']+"\t"+str(row['kind2']))
        #print(row['id']+"\t"+row['name']+"\t"+row['kind'])
        #data_json.append({"device_id":id_device,"course_id":row['id'],"course_name":row['name']})
    
    data = json.dumps(list_item)
    data = data.encode('utf-8')
    print(len(list_item))
    print(sys.getsizeof(data))
    num = 0
    if sys.getsizeof(data) > 100000:
        
        print('+++++++++++++++++++++++++++')
        n= round(sys.getsizeof(data)/100000)
        n1= round(len(list_item)/n)
        print(n1) 
        print(n) 
        for i in range(0,len(list_item),n1):
            print('************************')
            sub_list_item = []
            sub_data = []
            sub_list_item = list_item[i:i+n1]
            sub_data = json.dumps(sub_list_item)
            sub_data = sub_data.encode('utf-8')
            print(len(sub_list_item))
            print(sys.getsizeof(sub_data))
            future = publisher.publish(configSanMarcos.thread_topic_path, sub_data, **attributes)
            print(f'published message id {future.result()}')
    else:
        future = publisher.publish(configSanMarcos.thread_topic_path, data, **attributes)
        print(f'published message id {future.result()}')
        print(len(list_item))
        print(sys.getsizeof(data))

    #future = publisher.publish(configSanMarcos.thread_topic_path, data, **attributes)
    #print(f'published message id {future.result()}')

