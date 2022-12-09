import dao.DBManager

def insert(params):
    sql = "INSERT INTO monitor_process_item(monitor_process_id,num_items,gas_monitor_process_id,gas_monitor_process_item_id,state,created_at,updated_at) VALUES(?,?,?,?,?,DATETIME('now'),DATETIME('now'))"
    result = dao.DBManager.insertUpdateDelete(dao.DBManager.DB_LOCAL,sql,params)
    return result

def changeState(params):
    sql = "UPDATE monitor_process_item SET updated_at = DATETIME('now'),num_items=?,gas_monitor_process_item_id=?,gas_monitor_process_item_item_id=?,state = ? WHERE id = ?"
    result = dao.DBManager.insertUpdateDelete(dao.DBManager.DB_LOCAL,sql,params)
    return result

def find(id):        
    monitorProcessItem = None
    sql = "SELECT id,command,action,mode,gas_monitor_process_item_id,gas_monitor_process_item_item_id,state,created_at,updated_at FROM monitor_process_item WHERE id = '"+id+"' ORDER BY updated_at DESC limit 100"
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL,sql)
    if( len(result) > 0 ):
        monitorProcessItem = result[0]
    return monitorProcessItem   

def lastInserted():        
    monitorProcessItem = None
    sql = "SELECT id,monitor_process_id,num_items,gas_monitor_process_id,gas_monitor_process_item_id,state,created_at,updated_at FROM monitor_process_item ORDER BY id DESC limit 1"
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL,sql)
    if( len(result) > 0 ):
        monitorProcessItem = result[0]
    return monitorProcessItem   