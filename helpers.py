import subprocess
import config

def getSerialNumber():
    try:
        f = open(config.root_path+'/serial-number.txt','r')    
        serial_number = f.read()
        serial_number = serial_number.replace(" ","")
        serial_number = serial_number.replace("SerialNumber:","").strip().lstrip().rstrip()
        return serial_number
    except:
        print('No se pudo abrir el archivo de serial number BIOS')
        return None

def getSubscriptionThreadName():    
    serial_number = getSerialNumber()
    return "SM_THREAD_SUB_"+serial_number