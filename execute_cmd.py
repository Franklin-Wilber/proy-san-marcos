import sys
import json
import subscription
import helpers
import threadTransmission
import syncPeople

cmd_params = sys.argv
if( len(cmd_params) > 2 ):
    action = cmd_params[1]
    scrypt = cmd_params[2]
    if action == '--action':
        if scrypt == 'test-publish':
            data = {
                'message': "Mensaje recibido desde "+helpers.getSerialNumber()
            }
            threadTransmission.publish(str(data))
        elif scrypt == 'create-subscription':
            subscription.create()
        elif scrypt == 'sync-people':
            syncPeople.execute()
        else:
            print('La acciòn no existe ')
    else:
        print('El parametro '+action+' no es vàlido')
else:
    print("Paametros incorrectos")