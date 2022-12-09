import sys
import json
import subscription
import helpers
import config
import threadTransmission
import syncPeople
import dao.DBManager
import dao.MonitorProcessDAO
import dao.kolibri.ChannelDAO as channelDAO
import dao.kolibri.UserDAO as userDAO

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
        elif scrypt == 'create-database':
            dao.DBManager.createTablesIfNotExists()
        elif scrypt == 'receive-subscriptions':
            threadTransmission.sub( helpers.getSubscriptionThreadName() )
        elif scrypt == 'sync-people':
            syncPeople.execute()
        elif scrypt == 'list-process':
            state = 'ALL'
            list_process = dao.MonitorProcessDAO.getAll(state)
            if( len(list_process) > 0 ):
                line = "PROCESS ID\t\t\t\tCOMMAND\t\t\t\t\t\tACTION\t\tMODE\tSTATE\tCREATED_AT\t\tUPDATED_AT"
                print(line)
                print("********************************************************************************************************************************************************************")
                for p in list_process:
                    line = p[0]+"\t"+p[1]+"\t"+p[2]+"\t"+p[3]+"\t"+p[4]+"\t"+p[5]+"\t"+p[6]
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