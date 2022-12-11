import os
from google.cloud import pubsub_v1
import config
import helpers
import datetime
import json
import uuid
import csv
import codecs
import dao.MonitorProcessDAO
import dao.MonitorProcessItemDAO
import dao.kolibri.UserDAO as userKolibriDAO
import dao.StudyGroupDAO as studyGroupDAO
import dao.CourseDAO as courseDAO
import dao.PeopleDAO as peopleDAO
import dao.kolibri.KolibriAuthCollectionDAO as kolibriAuthCollectionDAO


def publish(status, action, data):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.file_credential_path
    PROJECT_ID = config.project_id
    PUB_THREAD_PY_REQUEST = config.PUB_THREAD_PY_REQUEST

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, PUB_THREAD_PY_REQUEST)
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
        data = codecs.decode(data, 'utf-8')
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
        gas_monitor_process_item_id = attributes.get(
            'gas_monitor_process_item_id')

        python_monitor_process_id = attributes.get('python_monitor_process_id')
        python_monitor_process_item_id = attributes.get(
            'python_monitor_process_item_id')

        print(python_monitor_process_id)
        print(python_monitor_process_item_id)

        status = attributes.get('status')

        resultData = json.loads(data, encoding='utf-8')

        if (device_id == helpers.getSerialNumber()):
            print('data ==> ')
            # message.ack()
            print('***********************')
            print(python_monitor_process_id)
            monitorProcess = None
            if (python_monitor_process_id == None):
                python_monitor_process_id = 0
            else:
                python_monitor_process_id = python_monitor_process_id

            if (int(python_monitor_process_id) > 0):
                monitorProcess = dao.MonitorProcessDAO.find(
                    python_monitor_process_id)
            else:
                params = ('python3 execute_cmd.py --action sync-people',
                          'sync-people', 'GAS', gas_monitor_process_id, 0, 0, 'PENDING')
                result = dao.MonitorProcessDAO.insert(params)
                if (result == True):
                    monitorProcess = dao.MonitorProcessDAO.lastInserted()
                    print('monitorProcess => Proceso creado')
                else:
                    print('monitorProcess => No se pudo crear el proceso')

            if (monitorProcess):
                userSuperAdmin = userKolibriDAO.getSuperAdmin()
                if (userSuperAdmin):
                    userSuperAdmin_id = userSuperAdmin["id"]
                    userSuperAdmin_facility_id = userSuperAdmin["facility_id"]
                    userSuperAdmin_dataset_id = userSuperAdmin["dataset_id"]
                    print(monitorProcess)
                    paramsItem = (python_monitor_process_id, part_num_total_items,
                                  gas_monitor_process_id, gas_monitor_process_item_id, 'PENDING')
                    resultItemState = dao.MonitorProcessItemDAO.insert(
                        paramsItem)
                    monitorProcessItem = dao.MonitorProcessItemDAO.lastInserted()
                    if (monitorProcessItem):
                        python_monitor_process_item_id = monitorProcessItem[0]

                    if (resultItemState):
                        params = (num_total_items, gas_monitor_process_id,
                                  gas_monitor_process_item_id, 'PENDING', python_monitor_process_id)
                        dao.MonitorProcessDAO.update(params)

                        if (action == 'sync-people-students'):
                            list_data = resultData['list_data']
                            list_users_array = []
                            list_users_array.append([
                                    "UUID",
                                    "USERNAME",
                                    "PASSWORD",
                                    "FULL_NAME",
                                    "USER_TYPE",
                                    "IDENTIFIER",
                                    "BIRTH_YEAR",
                                    "GENDER",
                                    "ENROLLED_IN",
                                    "ASSIGNED_TO"
                                ])

                            for item in list_data:
                                people_id = item['id']
                                people_uuid = item['data']['uuid']
                                study_group_code = item['data']['study_group_code']
                                campus_name = item['data']['campus_name']
                                grade_name = item['data']['grade_name']
                                level_name = item['data']['level_name']
                                section_name = item['data']['section_name']
                                updated_at = item['data']['updated_at']
                                user_type = "LEARNER"

                                existsUser = userKolibriDAO.findById(people_uuid)
                                if (existsUser != None):
                                    print(people_id+" existe")
                                    continue

                                course_index = 0
                                list_student_courses_str = ""
                                list_courses = courseDAO.getCoursesByStudyGroupCode(study_group_code)
                                for course in list_courses:
                                    if( course_index > 0 ):
                                        list_student_courses_str += ","
                                    list_student_courses_str += course[3]
                                    course_index = course_index + 1

                                print("==> "+people_id)
                                # print(people_uuid+" "+people_id + " " + campus_name + " " + grade_name + " " +
                                #       level_name + " " + section_name + " " + study_group_code + " " + user_type)
                                # print(list_student_courses_str)
                                user_id = None
                                user = peopleDAO.findByUsername(people_id)
                                if(user != None):
                                    user_id = user[0]

                                print(user_id)

                                row = [
                                    user_id,
                                    people_id,
                                    people_id,
                                    people_id,
                                    user_type,
                                    people_id,
                                    None,
                                    'NOT_SPECIFIED',
                                    list_student_courses_str,
                                    ""
                                ]
                                list_users_array.append(row)
                                
                            print(list_users_array)
                            
                            if len(list_users_array) > 1 :

                                path_import_files_people = config.DIRECTORY_PATH_FILES_IMPORT+"/files-people-"+str(uuid.uuid4())+".csv"
                                fileImportPeople = open(path_import_files_people, 'w')
                                with fileImportPeople:
                                    writer = csv.writer(fileImportPeople)
                                    writer.writerows(list_users_array)

                                cmd = "kolibri manage bulkimportusers "+path_import_files_people
                                print(cmd)
                                # exit()
                                cmd_import_teachers_and_students = os.system(cmd)
                                print(cmd_import_teachers_and_students)

                                print("*******************")
                                # exit()

                        elif (action == 'sync-courses'):
                            num_courses_new = 0
                            list_data = resultData['list_data']
                            for item in list_data:
                                course_id = item['id']
                                course_uuid = item['data']['uuid']
                                course_code = item['data']['course_code']
                                course_name = item['data']['course_name']
                                study_group_code = item['data']['study_group_code']
                                campus_name = item['data']['campus_name']
                                level_name = item['data']['level_name']
                                grade_name = item['data']['grade_name']
                                section_name = item['data']['section_name']
                                
                                # collection_id = str(uuid.uuid4()).replace("-","")
                                collection_id = course_uuid
                                collection_name = course_name + " - "+campus_name+" - "+level_name+" - "+grade_name+" - "+section_name
                                parent_id = userSuperAdmin_facility_id
                                _morango_source_id = str(uuid.uuid4()).replace("-","")
                                _morango_partition = userSuperAdmin_dataset_id+":allusers-ro"
                                collection = { "id": collection_id,"name": collection_name,"kind": "classroom","dataset_id": userSuperAdmin_dataset_id,"parent_id": parent_id,"_morango_dirty_bit": 1,"_morango_source_id": _morango_source_id,"_morango_partition": _morango_partition }

                                existsCourse = kolibriAuthCollectionDAO.findById(course_uuid)
                                if existsCourse == None :
                                    num_courses_new = num_courses_new + 1
                                else:
                                    continue
                                
                                studGroup = studyGroupDAO.findById(study_group_code)
                                if( studGroup == None ):
                                    studyGroupDAO.insert( (study_group_code,0,campus_name,level_name,grade_name,section_name,python_monitor_process_id,python_monitor_process_item_id) )
                                    studGroup = studyGroupDAO.findById(study_group_code)

                                course = courseDAO.findById(course_code)
                                if( course == None ):
                                    courseDAO.insert( (course_code,course_uuid,course_name,collection_name,study_group_code,python_monitor_process_id,python_monitor_process_item_id,'PROCESSED') )

                                kolibriAuthCollectionDAO.insert(collection)
                                print(course_id + " " + collection_id)
                            print(str( len(list_data))+" cursos")

                            print("Se crearon "+str(num_courses_new)+" cursos")
                        else:
                            print('Opciòn no vàlida')
                                
                        params = ('PROCESSED',python_monitor_process_item_id)
                        dao.MonitorProcessItemDAO.changeState( params )                        

                        params = (num_total_items, gas_monitor_process_id,
                                  gas_monitor_process_item_id, 'PENDING', python_monitor_process_id)
                        dao.MonitorProcessDAO.update(params)
                        dao.MonitorProcessDAO.changeState( python_monitor_process_id )
                        
                        message.ack()
                        print("************ ************ ************ ************ ************")
                    else:
                        print("No se registroo el item de proceso")
                else:
                    print("No se encontrò una cuenta admin dentro de kolibrì")
        else:
            print("No hay data por procesar")
        
        message.ack()

    subscription_path = subscriber.subscription_path(
        config.project_id, subscription)
    print(subscription_path)

    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=callback)
    print(f'Listening for messages on {subscription_path}')

    with subscriber:
        try:
            streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()
