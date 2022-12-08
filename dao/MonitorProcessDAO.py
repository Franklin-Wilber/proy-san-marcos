import dao.DBManager

def insertEvent(params):
    sql = "INSERT INTO monitor_process(command,action,mode,num_items,state,runned_at,updated_at) VALUES(?,?,?,?,?,DATETIME('now'),DATETIME('now'))"
    result = dao.DBManager.insertUpdateDelete(dao.DBManager.DB_LOCAL,sql,params)
    return result

def changeStateEvent(params):
    sql = "UPDATE monitor_process SET updated_at = DATETIME('now'),num_items=?,state = ? WHERE id = ?"
    result = dao.DBManager.insertUpdateDelete(dao.DBManager.DB_LOCAL,sql,params)
    return result    

def getAll(state):
    where_state = ""
    if( state == 'PENDING' ):
        where_state = "WHERE  state = 'PENDING'"
    elif( state == 'PROCESSED' ):
        where_state = "WHERE  state = 'PROCESSED'"
        
    sql = "SELECT id,command,action,mode,state,runned_at,updated_at FROM monitor_process "+where_state+" ORDER BY updated_at DESC limit 100"
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL,sql)
    return result

def find(id):        
    sql = "SELECT id,command,action,mode,state,runned_at,updated_at FROM monitor_process WHERE id = '"+id+"' ORDER BY updated_at DESC limit 100"
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL,sql)
    return result

def lastInserted():        
    sql = "SELECT id,command,action,mode,state,runned_at,updated_at FROM monitor_process ORDER BY id DESC limit 1"
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL,sql)
    return result    