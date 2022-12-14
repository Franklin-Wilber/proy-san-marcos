import os
from google.cloud import pubsub_v1
import config
import datetime
import json
import helpers
import dao.DBManager
import config
import pandas as pd
import sqlite3
import sys
from sqlite3 import Error


def listCourse(id_device):
    list_users = []
    try:
        query = "SELECT id,name,kind FROM kolibriauth_collection where kind='classroom'"
        result = dao.DBManager.executeResult(dao.DBManager.DB_KOLIBRI,query )     
        
        for row in result:
            #print(row[0]+"\t"+row[1]+"\t"+row[2])
            list_users.append({"device_id":id_device,"course_id":row[0],"course_name":row[1]})
        return list_users
    except Error as e:
        print(e)
    finally:
        print('Consulta realizada')

def listUser(id_device):
    list_item = []
    try:
        query = "SELECT id,full_name FROM kolibriauth_facilityuser"
        result = dao.DBManager.executeResult(dao.DBManager.DB_KOLIBRI,query )    
        for row in result:
            #print(row[0]+"\t"+row[1])
            list_item.append({"device_id":id_device,"user_id":row[0],"user_name":row[1]})
        return list_item
    except Error as e:
        print(e)
    finally:
        print('Consulta realizada')

def listLesson(id_device,start,end):
    list_item = []
    try:
        query = "select teacher.id,teacher.full_name, strftime('%Y/%m/%d',SUBSTRING(l.date_created, 1, 10)) as anio, l.id, l.title, l.collection_id, course.name from lessons_lesson as l INNER JOIN kolibriauth_facilityuser as teacher ON l.created_by_id=teacher.id INNER JOIN kolibriauth_collection as course ON course.id = l.collection_id where anio>='"+start+"' and anio<='"+end+"' ORDER BY anio"
        result = dao.DBManager.executeResult(dao.DBManager.DB_KOLIBRI,query)     
        
        for row in result:
            #print(row[0]+"\t"+row[1]+"\t"+row[2]+"\t"+row[3]+"\t"+row[4]+"\t"+row[5]+"\t"+row[6])
            list_item.append({ "device_id": id_device,"teacher_id": row[0],"teacher_name": row[1],"date_create": row[2], "lesson_id": row[3],"lesson_name": row[4],"course_id": row[5],"course_name": row[6]})
         
        return list_item
    except Error as e:
        print(e)
    finally:
        print('Consulta realizada')

def listDataLesson(id_device,start,end):
    list_item = []
    try:
        query = "select teacher.id,teacher.full_name, strftime('%Y/%m/%d',SUBSTRING(l.date_created, 1, 10)) as anio, strftime('%m',SUBSTRING(l.date_created, 1, 10)) as mes, count(teacher.id) as numero_lesson from lessons_lesson as l INNER JOIN kolibriauth_facilityuser as teacher ON l.created_by_id=teacher.id where anio>='"+start+"' and anio<='"+end+"' Group By anio,teacher.id ORDER BY anio"
        result = dao.DBManager.executeResult(dao.DBManager.DB_KOLIBRI,query)     
        
        for row in result:
            #print(row[0]+"\t"+row[1]+"\t"+row[2]+"\t"+str(row[4]))
            list_item.append({ "device_id": id_device,"user_id": row[0],"full_name": row[1],"num_lesson": str(row[4]),"start_date": start,"end_date": end})
         
        return list_item
    except Error as e:
        print(e)
    finally:
        print('Consulta realizada')

def frecuencyChannel(id_device,start,end):
    list_item = []
    list_lesson_lesson_id = []
    list_lesson_lesson_name = []
    list_lesson_channel_id = []
    list_lesson_channel_name = []
    list_cant_channel = []
    try:
        for x in range(6):
            query = "SELECT l.id as lesson_id, l.title, JSON_EXTRACT(l.resources,'$["+str(x)+"].channel_id') AS canal_id,c.name as nombre_canal, strftime('%Y/%m/%d',SUBSTRING(l.date_created, 1, 10)) as anio FROM lessons_lesson as l INNER join content_channelmetadata as c ON canal_id=c.id where anio>='"+start+"' and anio<='"+end+"' and canal_id IS NOT NULL"
            result = dao.DBManager.executeResult(dao.DBManager.DB_KOLIBRI,query) 
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
            #print(index[row][0]+"\t"+index[row][1]+"\t"+str(cantidad[row][0] ))
            list_cant_channel.append({ "device_id": id_device,"channel_id":index[row][0],"channel_name":index[row][1],"cantidad": str(cantidad[row][0]),"start_date": start,"end_date": end})
        return list_cant_channel
    except Error as e:
        print(e)
    finally:
        print('Consulta realizada')

def frecuencyLessonPerTeacher(id_device,start,end):
    list_item = []
    try:
        query = "select teacher.id,teacher.full_name, strftime('%Y/%m/%d',SUBSTRING(l.date_created, 1, 10)) as anio, count(teacher.id) as numero_lesson from lessons_lesson as l INNER JOIN kolibriauth_facilityuser as teacher ON l.created_by_id=teacher.id where anio>='"+start+"' and anio<='"+end+"' Group By anio, teacher.id ORDER BY anio"
        result = dao.DBManager.executeResult(dao.DBManager.DB_KOLIBRI,query)    
        
        for row in result:
            #print(row[0]+"\t"+row[1]+"\t"+row[2]+"\t"+str( row[3]))
            list_item.append({ "device_id": id_device,"teacher_id": row[0],"teacher_name": row[1],"anio": row[2],"cantidad":str( row[3]),"start_date": input1,"end_date": input2 })
         
        return list_item
    except Error as e:
        print(e)
    finally:
        print('Consulta realizada')

def node(id_device):
    list_item = []
    try:
        query = "select id,title,channel_id,description from content_contentnode limit(3000)"
        result = dao.DBManager.executeResult(dao.DBManager.DB_KOLIBRI,query) 
        
        for row in result:
            #print(row[0]+"\t"+row[1]+"\t"+row[2]+"\t"+ row[3])
            list_item.append({ "device_id": id_device,"node_id": row[0],"title": row[1],"channel_id": row[2],"description": ''})
        
        return list_item
    except Error as e:
        print(e)
    finally:
        print('Consulta realizada')

def publishMetaData(option,input1,input2):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.file_credential_path

    publisher = pubsub_v1.PublisherClient()

    date_time = datetime.datetime.now()
    date = date_time.strftime("%Y/%m/%d")
    id_device = helpers.getSerialNumber()

    attributes = {}

    list_item = []
    if option == 'course':
        attributes = {
            'table': 'course',
        }
        list_item = listCourse(id_device)
    elif option == 'lesson-data':
        attributes = {
            'table': 'lesson-data',
        }
        list_item = listDataLesson(id_device,input1,input2)
    elif option == 'lesson':
        attributes = {
            'table': 'lesson',
        }
        list_item = listLesson(id_device,input1,input2)
    elif option == 'channel':
        attributes = {
            'table': 'channel',
        }
        list_item = frecuencyChannel(id_device,input1,input2)
    elif option == 'teacher-lesson':
        attributes = {
            'table': 'teacher',
        }
        list_item = frecuencyLessonPerTeacher(id_device,input1,input2)

    elif option == 'node':
        attributes = {
            'table': 'node',
        }
        list_item = node(id_device)

    elif option == 'user':
        attributes = {
            'table': 'user',
        }
        list_item = listUser(id_device)
    
    data = json.dumps(list_item)
    data = data.encode('utf-8')
    if sys.getsizeof(data) > 100000:
        n= round(sys.getsizeof(data)/100000)
        range= round(len(list_item)/n)
        for i in range(0,len(list_item),range):
            print('************************')
            sub_list_item = []
            sub_data = []
            sub_list_item = list_item[i:i+range]
            sub_data = json.dumps(sub_list_item)
            sub_data = sub_data.encode('utf-8')
            future = publisher.publish(config.metadata_topic_path, sub_data, **attributes)
            print(f'published message id {future.result()}')
    else:
        print('2')
        future = publisher.publish(config.metadata_topic_path, data, **attributes)
        print(f'published message id {future.result()}')


