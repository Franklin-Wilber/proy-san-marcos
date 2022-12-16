import sys
import json
import subscription
import helpers
import config
import threadTransmission
import syncProcess
import dao.DBManager
import dao.MonitorProcessDAO
import dao.kolibri.ChannelDAO as channelDAO
import dao.kolibri.UserDAO as userDAO
import dao.metadataPublish as metadataPublish

cmd_params = sys.argv
if( len(cmd_params) > 2 ):
    action = cmd_params[1]
    scrypt = cmd_params[2]

    if action == '--action':
        if scrypt == 'test-publish':
            data = {
                'message': "Mensaje recibido desde "+helpers.getSerialNumber()
            }
            threadTransmission.publish('INFO','test-publish',str(data))
        elif scrypt == 'create-subscription':
            subscription.create()
        elif scrypt == 'publish-metadata':
            option = ''
            input1 = ''
            input2 = ''
            try:
                option = cmd_params[3]
            except:
                print('')
            try:
                input1 = cmd_params[4]
            except:
                print('')
            try:
                input2 = cmd_params[5]
            except:
                print('')
            metadataPublish.publishMetaData(option,input1,input2)
        elif scrypt == 'create-database':
            dao.DBManager.createTablesIfNotExists()
        elif scrypt == 'receive-subscriptions':
            threadTransmission.sub( helpers.getSubscriptionThreadName() )
        elif scrypt == 'sync-people-students':
            syncProcess.executeCommand('sync-people-students','COMMAND')
        elif scrypt == 'sync-people-teachers':
            syncProcess.executeCommand('sync-people-teachers','COMMAND')
        elif scrypt == 'sync-courses':
            syncProcess.executeCommand('sync-courses','COMMAND')
        elif scrypt == 'list-process':
            state = 'ALL'
            list_process = dao.MonitorProcessDAO.getAll(state)
            if( len(list_process) > 0 ):
                line = "PROCESS ID\t\t\t\tCOMMAND\t\t\tACTION\t\tMODE\t\tSTATE\t\tCREATED_AT\t\t\tUPDATED_AT"
                print(line)
                print("********************************************************************************************************************************************************************")
                for p in list_process:
                    line = str(p[0])+"\t\t"+str(p[1])+"\t"+str(p[2])+"\t"+str(p[3])+"\t\t"+str(p[7])+"\t\t"+str(p[8])+"\t\t"+str(p[9])
                    print(line)
            else:
                print('No hay procesos actualmente')        
        elif scrypt == 'kolibri-super-admin':
            superAdmin = userDAO.getSuperAdmin()
            print(superAdmin)

        else:
            print('La acciòn no existe ')
    else:
        print('El parametro '+action+' no es vàlido')
else:
    print("Paametros incorrectos")