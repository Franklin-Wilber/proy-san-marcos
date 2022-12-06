import threadTransmission
import uuid
import json
import dao.MonitorProcessDAO


def execute():
    print('Solicitando sincronizaciòn...')
    id = str(uuid.uuid4())
    params = (id, 'python3 execute_cmd.py --action sync-people',
              'sync-people', 'MANUAL', 'PENDING')
    result = dao.MonitorProcessDAO.insertEvent(params)
    if (result == True):
        print(" ** Solicitud creada, el procesamiento puede demorar varios minutos en efectuarse..")
        print(" ** ID = "+id)
        processResult = dao.MonitorProcessDAO.find(id)
        if (len(processResult) > 0):
            processRow = processResult[0]
            process = {"id": processRow[0], "command": processRow[1], "type": processRow[2], "mode": processRow[3],
                       "state": processRow[4], "runned_at": processRow[5], "updated_at": processRow[6]}
            processStr = json.dumps(process)
            threadTransmission.publish('SUCCESS', process['type'], processStr)
        else:
            print("Proceso no encontrado")
    else:
        print("No se pudo realizar la peticiòn")
    return False
