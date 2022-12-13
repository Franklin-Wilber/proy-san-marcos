import threadTransmission
import uuid
import json
import dao.MonitorProcessDAO


def xxxxxxxxxxexecute():
    print('Solicitando sincronizaciòn...')
    params = ('python3 execute_cmd.py --action sync-people',
              'sync-people', 'COMMAND','0','0','0', 'PENDING')
    result = dao.MonitorProcessDAO.insert(params)
    if (result == True):
        print(" ** Solicitud creada, el procesamiento puede demorar varios minutos en efectuarse..")
        processRow = dao.MonitorProcessDAO.lastInserted()
        if (processRow):
            print(" ** ID = "+str(processRow[0]))
            process = {"id": processRow[0], "command": processRow[1], "action": processRow[2], "mode": processRow[3],
                       "state": processRow[4], "created_at": processRow[5], "updated_at": processRow[6]}
            processStr = json.dumps(process)
            threadTransmission.publish(
                'SUCCESS', process['action'], processStr)
        else:
            print("Proceso no encontrado")
    else:
        print("No se pudo realizar la peticiòn")
    return False
