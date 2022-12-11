import dao.DBManager

def insert(params):
    sql = "INSERT INTO monitor_process(command,action,mode,gas_monitor_process_id,gas_monitor_process_item_id,num_items,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,DATETIME('now'),DATETIME('now'))"
    result = dao.DBManager.insertUpdateDelete(dao.DBManager.DB_LOCAL,sql,params)
    return result

def update(params):
    sql = "UPDATE monitor_process SET updated_at = DATETIME('now'),num_items=?,gas_monitor_process_id=?,gas_monitor_process_item_id=?,state = ? WHERE id = ?"
    result = dao.DBManager.insertUpdateDelete(dao.DBManager.DB_LOCAL,sql,params)
    return result

def changeState(id):
    sql = ""
    sql += "SELECT "
    sql += "( SELECT count(1) FROM monitor_process_item mpi WHERE mpi.monitor_process_id = mp.id AND mpi.state = 'PROCESSED' ) as mpi_success, "
    sql += "( SELECT count(1) FROM monitor_process_item mpi WHERE mpi.monitor_process_id = mp.id AND mpi.state = 'ERROR' ) as mpi_error, "
    sql += "( SELECT count(1) FROM monitor_process_item mpi WHERE mpi.monitor_process_id = mp.id AND mpi.state = 'PENDING' ) as mpi_pending "
    sql += "FROM monitor_process mp WHERE mp.id = '"+id+"' "
    monitorProcessResult = None
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL,sql)
    if( len(result) > 0 ):
        monitorProcess = result[0]
        num_success = monitorProcess[0]
        num_error = monitorProcess[1]
        num_pending = monitorProcess[2]

        state = "PENDING"
        sql = ""
        if( num_pending > 0 ):
            state = "PENDING"
        elif( num_success <= num_error ):
            state = "FAIL"
        elif( num_success > num_error ):
            state = "SUCCESS"

        sql = "UPDATE monitor_process SET updated_at = DATETIME('now'),state = ? WHERE id = ?"
        b = dao.DBManager.insertUpdateDelete(dao.DBManager.DB_LOCAL,sql, (state,id) )
        print(b)
    return monitorProcessResult
    # sql = "UPDATE monitor_process SET updated_at = DATETIME('now'),num_items=?,gas_monitor_process_id=?,gas_monitor_process_item_id=?,state = ? WHERE id = ?"
    # result = dao.DBManager.insertUpdateDelete(dao.DBManager.DB_LOCAL,sql, (id) )
    # return result

def getAll(state):
    where_state = ""
    if( state == 'PENDING' ):
        where_state = "WHERE  state = 'PENDING'"
    elif( state == 'PROCESSED' ):
        where_state = "WHERE  state = 'PROCESSED'"

    sql = "SELECT id,command,action,mode,gas_monitor_process_id,gas_monitor_process_item_id,num_items,state,created_at,updated_at FROM monitor_process "+where_state+" ORDER BY updated_at DESC limit 100"
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL,sql)
    return result

def find(id):        
    monitorProcess = None
    sql = "SELECT id,command,action,mode,gas_monitor_process_id,gas_monitor_process_item_id,num_items,state,created_at,updated_at FROM monitor_process WHERE id = '"+id+"' ORDER BY updated_at DESC limit 100"
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