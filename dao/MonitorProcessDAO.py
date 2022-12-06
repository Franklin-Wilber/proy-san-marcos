import dao.DBManager

def insertEvent(params):
    sql = "INSERT INTO monitor_process(id,command,type,mode,state,runned_at,updated_at) VALUES(?,?,?,?,?,DATETIME('now'),DATETIME('now'))"
    result = dao.DBManager.insertUpdateDelete(dao.DBManager.DB_LOCAL,sql,params)
    return result

def getAll(state):
    where_state = ""
    if( state == 'PENDING' ):
        where_state = "WHERE  state = 'PENDING'"
    elif( state == 'PROCESSED' ):
        where_state = "WHERE  state = 'PROCESSED'"
        
    sql = "SELECT id,command,type,mode,state,runned_at,updated_at FROM monitor_process "+where_state+" ORDER BY updated_at DESC limit 100"
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL,sql)
    return result

def find(id):        
    sql = "SELECT id,command,type,mode,state,runned_at,updated_at FROM monitor_process WHERE id = '"+id+"' ORDER BY updated_at DESC limit 100"
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL,sql)
    return result    