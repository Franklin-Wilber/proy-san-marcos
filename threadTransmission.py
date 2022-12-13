import os
from google.cloud import pubsub_v1
import config
import helpers
import datetime
import json
import uuid
import csv
import codecs
import dao.DBManager
import dao.MonitorProcessDAO
import dao.MonitorProcessItemDAO
import dao.kolibri.UserDAO as userKolibriDAO
import dao.StudyGroupDAO as studyGroupDAO
import dao.CourseDAO as courseDAO
import dao.PeopleDAO as peopleDAO
import dao.ImportTeachersDAO as importTeachersDAO
import dao.kolibri.KolibriAuthCollectionDAO as kolibriAuthCollectionDAO
import syncProcess


def publish(status, action, data):
    try:
        config.showError("MODE : Publish")
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.file_credential_path
        PROJECT_ID = config.project_id
        PUB_THREAD_PY_REQUEST = config.PUB_THREAD_PY_REQUEST

        config.showError(PUB_THREAD_PY_REQUEST)

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
        config.showError(attributes)
        config.showError(data)
        config.showError(f'PUBLISHED MESSAGE ID = {future.result()}')
        print(f'PUBLISHED MESSAGE ID = {future.result()}')
    except Exception as err:
        print(err)
        config.showError(err)
    finally:
        config.showError("-----------")


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

        status = attributes.get('status')

        resultData = json.loads(data, encoding='utf-8')

        if (device_id == helpers.getSerialNumber()):
            # message.ack()
            print('***********************  ' +
                  action+'  ***********************')

            config.showError("ACTION = "+action)

            monitorProcess = None
            if (python_monitor_process_id == None):
                python_monitor_process_id = 0
            else:
                python_monitor_process_id = python_monitor_process_id

            if (int(python_monitor_process_id) > 0):
                monitorProcess = dao.MonitorProcessDAO.find(
                    python_monitor_process_id)
            else:
                params = ('python3 execute_cmd.py --action '+action,
                          action, 'GAS', gas_monitor_process_id, 0, 0, 'PENDING')
                result = dao.MonitorProcessDAO.insert(params)

                if (result == True):
                    monitorProcess = dao.MonitorProcessDAO.lastInserted()
                    print('monitorProcess => Proceso creado')
                else:
                    print('monitorProcess => No se pudo crear el proceso')

            if (monitorProcess):
                config.showError(monitorProcess)

                userSuperAdmin = userKolibriDAO.getSuperAdmin()
                if (userSuperAdmin == None):
                    print("No se encontrò una cuenta admin dentro de kolibrì")
                    config.showError("No se encontrò una cuenta admin dentro de kolibrì")
                else:
                    userSuperAdmin_id = userSuperAdmin["id"]
                    userSuperAdmin_facility_id = userSuperAdmin["facility_id"]
                    userSuperAdmin_dataset_id = userSuperAdmin["dataset_id"]                    
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

                        if (action == 'test-publish'):
                            result_message = resultData['list_data']
                            sendInfo(action, python_monitor_process_id,
                                     gas_monitor_process_id, result_message)
                        elif (action == 'create-database'):
                            dao.DBManager.createTablesIfNotExists()
                            result_message = "Base de datos generada con èxito"
                            sendInfo(action, python_monitor_process_id,
                                     gas_monitor_process_id, result_message)
                        elif (action == 'sync-people-students'):
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
                                updated_at = item['data']['updated_at']
                                user_type = "LEARNER"

                                existsUser = userKolibriDAO.findById(
                                    people_uuid)
                                if (existsUser != None):
                                    print(people_id+" existe")
                                    continue

                                course_index = 0
                                list_student_courses_str = ""
                                list_courses = courseDAO.getCoursesByStudyGroupCode(
                                    study_group_code)
                                for course in list_courses:
                                    if (course_index > 0):
                                        list_student_courses_str += ","
                                    list_student_courses_str += course[3]
                                    course_index = course_index + 1

                                # print("==> "+people_id)
                                # print(people_uuid+" "+people_id + " " + campus_name + " " + grade_name + " " +
                                #       level_name + " " + section_name + " " + study_group_code + " " + user_type)
                                # print(list_student_courses_str)
                                user_id = None
                                user = peopleDAO.findByUsername(people_id)
                                if (user != None):
                                    user_id = user[0]
                                    # print(user_id)

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
                                if (len(list_courses) > 0):
                                    list_users_array.append(row)

                            if len(list_users_array) > 1:
                                config.showError("Se procesaràn "+str( len(list_users_array) )+" items")
                                path_import_files_people = config.DIRECTORY_PATH_FILES_IMPORT + \
                                    "/files-people-"+str(uuid.uuid4())+".csv"
                                fileImportPeople = open(
                                    path_import_files_people, 'w')
                                with fileImportPeople:
                                    writer = csv.writer(fileImportPeople)
                                    writer.writerows(list_users_array)

                                cmd = "kolibri manage bulkimportusers "+path_import_files_people
                                print(cmd)
                                config.showError(path_import_files_people)
                                config.showError(cmd)
                                os.system(cmd)
                                # exit()
                            else:
                                config.showError("No se registroo el item de proceso")

                        elif (action == 'sync-people-teachers'):
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
                                teacher_id = str(item['id'])
                                course_code = item['data']['course_code']
                                study_group_code = item['data']['study_group_code']
                                teacher_code = item['data']['teacher_code']

                                importTeachers = importTeachersDAO.findIfExists(
                                    course_code, study_group_code, teacher_code)
                                if (importTeachers == None):
                                    importTeachersDAO.insert((course_code, study_group_code, teacher_code, "name - "+teacher_code, "lastname - " +
                                                             teacher_code, "fullname - "+teacher_code, python_monitor_process_id, python_monitor_process_item_id, 'PROCCESED'))
                                    importTeachers = importTeachersDAO.findIfExists(
                                        course_code, study_group_code, teacher_code)

                            list_teachers = importTeachersDAO.getTeachers(
                                python_monitor_process_id, python_monitor_process_item_id)
                            for item in list_teachers:
                                # print(item)
                                teacher_code = str(item[0])
                                name = str(item[0])
                                lastname = str(item[1])
                                fullname = str(item[2])
                                list_courses = importTeachersDAO.getCoursesByTeachers(
                                    teacher_code)
                                # print("----------------------")

                                course_index = 0
                                list_teachers_courses_str = ""
                                for course in list_courses:
                                    if (course_index > 0):
                                        list_teachers_courses_str += ","
                                    list_teachers_courses_str += course[6]
                                    course_index = course_index + 1

                                user_id = None
                                user = peopleDAO.findByUsername(teacher_code)
                                if (user != None):
                                    user_id = user[0]

                                user_type = "CLASS_COACH"

                                row = [
                                    user_id,
                                    teacher_code,
                                    teacher_code,
                                    fullname,
                                    user_type,
                                    user_id,
                                    None,
                                    'NOT_SPECIFIED',
                                    "",
                                    list_teachers_courses_str
                                ]
                                if (len(list_courses) > 0):
                                    list_users_array.append(row)
                                    # print(row)

                            if len(list_users_array) > 1:
                                config.showError("Se procesaràn "+str( len(list_users_array) )+" items")
                                path_import_files_people = config.DIRECTORY_PATH_FILES_IMPORT + \
                                    "/files-people-teachers-" + \
                                    str(uuid.uuid4())+".csv"
                                fileImportPeople = open(
                                    path_import_files_people, 'w')
                                with fileImportPeople:
                                    writer = csv.writer(fileImportPeople)
                                    writer.writerows(list_users_array)

                                cmd = "kolibri manage bulkimportusers "+path_import_files_people
                                print(cmd)
                                config.showError(path_import_files_people)
                                config.showError(cmd)
                                os.system(cmd)
                                # print(cmd_import_teachers_and_students)
                            else:
                                config.showError("No se registroo el item de proceso")

                        elif (action == 'sync-courses'):
                            num_courses_new = 0
                            list_data = resultData['list_data']
                            config.showError("Se procesaràn "+str(list_data)+" cursos")
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
                                collection_name = course_name + " - "+campus_name + \
                                    " - "+level_name+" - "+grade_name+" - "+section_name
                                parent_id = userSuperAdmin_facility_id
                                _morango_source_id = str(
                                    uuid.uuid4()).replace("-", "")
                                _morango_partition = userSuperAdmin_dataset_id+":allusers-ro"
                                collection = {"id": collection_id, "name": collection_name, "kind": "classroom", "dataset_id": userSuperAdmin_dataset_id,
                                              "parent_id": parent_id, "_morango_dirty_bit": 1, "_morango_source_id": _morango_source_id, "_morango_partition": _morango_partition}

                                existsCourse = kolibriAuthCollectionDAO.findById(
                                    course_uuid)
                                if existsCourse == None:
                                    num_courses_new = num_courses_new + 1
                                else:
                                    continue

                                studGroup = studyGroupDAO.findById(
                                    study_group_code)
                                if (studGroup == None):
                                    studyGroupDAO.insert((study_group_code, 0, campus_name, level_name, grade_name,
                                                         section_name, python_monitor_process_id, python_monitor_process_item_id))
                                    studGroup = studyGroupDAO.findById(
                                        study_group_code)

                                course = courseDAO.findById(course_code)
                                if (course == None):
                                    courseDAO.insert((course_code, course_uuid, course_name, collection_name, study_group_code,
                                                     python_monitor_process_id, python_monitor_process_item_id, 'PROCESSED'))

                                kolibriAuthCollectionDAO.insert(collection)
                                # print(course_id + " " + collection_id)
                            print(str(len(list_data))+" cursos")

                            print("Se crearon "+str(num_courses_new)+" cursos")
                            config.showError("Se crearon "+str(num_courses_new)+" cursos")
                        else:
                            print('Opciòn no vàlida')

                        params = ('PROCESSED', python_monitor_process_item_id)
                        dao.MonitorProcessItemDAO.changeState(params)

                        params = (num_total_items, gas_monitor_process_id,
                                  gas_monitor_process_item_id, 'PENDING', python_monitor_process_id)
                        dao.MonitorProcessDAO.update(params)
                        dao.MonitorProcessDAO.changeState(
                            python_monitor_process_id)

                        message.ack()
                    else:                        
                        config.showError("No se registroo el item de proceso")
            else:
                config.showError("No se creò el proceso")
            print("********     FIN     ***********")
            config.showError("********     FIN     ***********")
        else:
            print("No hay data por procesar")
            config.showError("No hay data por procesar")

        message.ack()

    subscription_path = subscriber.subscription_path(
        config.project_id, subscription)
    print(subscription_path)
    config.showError(subscription_path)
    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=callback)
    print(f'Listening for messages on {subscription_path}')
    config.showError(f'Listening for messages on {subscription_path}')
    with subscriber:
        try:
            streaming_pull_future.result()
        except TimeoutError as err:
            config.showError(err)
            streaming_pull_future.cancel()
            streaming_pull_future.result()


def sendInfo(action, python_monitor_process_id, gas_monitor_process_id, result_message):
    date = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    result_message = {"action": "info-device", "action_source": action, "mode": "GAS",
                      "python_monitor_process_id": python_monitor_process_id, "gas_monitor_process_id": gas_monitor_process_id,
                      "state": "SUCCESS", "created_at": date, "updated_at": date, "result_message": result_message}
    processStr = json.dumps(result_message)
    config.showError("Result : "+processStr)
    publish('INFO', action, processStr)
