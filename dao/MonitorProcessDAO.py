import dao.DBManager

def insert(params):
    sql = "INSERT INTO monitor_process(command,action,mode,gas_monitor_process_id,gas_monitor_process_item_id,num_items,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,DATETIME('now'),DATETIME('now'))"
    result = dao.DBManager.insertUpdateDelete(dao.DBManager.DB_LOCAL,sql,params)
    return result

def changeState(params):
    sql = "UPDATE monitor_process SET updated_at = DATETIME('now'),num_items=?,gas_monitor_process_id=?,gas_monitor_process_item_id=?,state = ? WHERE id = ?"
    result = dao.DBManager.insertUpdateDelete(dao.DBManager.DB_LOCAL,sql,params)
    return result

def getAll(state):
    where_state = ""
    if( state == 'PENDING' ):
        where_state = "WHERE  state = 'PENDING'"
    elif( state == 'PROCESSED' ):
        where_state = "WHERE  state = 'PROCESSED'"

    sql = "SELECT id,command,action,mode,gas_monitor_process_id,gas_monitor_process_item_id,state,created_at,updated_at FROM monitor_process "+where_state+" ORDER BY updated_at DESC limit 100"
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL,sql)
    return result

def find(id):        
    monitorProcess = None
    sql = "SELECT id,command,action,mode,gas_monitor_process_id,gas_monitor_process_item_id,state,created_at,updated_at FROM monitor_process WHERE id = '"+id+"' ORDER BY updated_at DESC limit 100"
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL,sql)
    if( len(result) > 0 ):
        monitorProcess = result[0]
    return monitorProcess   

def lastInserted():        
    monitorProcess = None
    sql = "SELECT id,command,action,mode,gas_monitor_process_id,gas_monitor_process_item_id,state,created_at,updated_at FROM monitor_process ORDER BY id DESC limit 1"
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL,sql)
    if( len(result) > 0 ):
        monitorProcess = result[0]
    return monitorProcess   