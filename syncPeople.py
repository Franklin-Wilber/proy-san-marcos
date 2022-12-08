import threadTransmission
import uuid
import json
import dao.MonitorProcessDAO


def execute():
    print('Solicitando sincronizaciòn...')
    params = ('python3 execute_cmd.py --action sync-people',
              'sync-people', 'MANUAL', 0, 'PENDING')
    result = dao.MonitorProcessDAO.insertEvent(params)
    if (result == True):
        print(" ** Solicitud creada, el procesamiento puede demorar varios minutos en efectuarse..")
        processResult = dao.MonitorProcessDAO.lastInserted()
        if (len(processResult) > 0):
            processRow = processResult[0]
            print(" ** ID = "+str(processRow[0]))
            process = {"id": processRow[0], "command": processRow[1], "action": processRow[2], "mode": processRow[3],
                       "state": processRow[4], "runned_at": processRow[5], "updated_at": processRow[6]}
            processStr = json.dumps(process)
            threadTransmission.publish(
                'SUCCESS', process['action'], processStr)
        else:
            print("Proceso no encontrado")
    else:
        print("No se pudo realizar la peticiòn")
    return False
