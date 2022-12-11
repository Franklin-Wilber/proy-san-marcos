import dao.DBManager


def insert(params):
    sql = "INSERT INTO course(code,uuid,name,fullname,study_group_code,monitor_process_id,monitor_process_item_id,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,DATETIME('now'),DATETIME('now'))"
    result = dao.DBManager.insertUpdateDelete(
        dao.DBManager.DB_LOCAL, sql, params)
    return result


def update(params):
    sql = "UPDATE monitor_process SET updated_at = DATETIME('now'),num_items=?,gas_monitor_process_id=?,gas_monitor_process_item_id=?,state = ? WHERE id = ?"
    result = dao.DBManager.insertUpdateDelete(
        dao.DBManager.DB_LOCAL, sql, params)
    return result


def findById(id):
    sql = "SELECT code,uuid,name,fullname,study_group_code,monitor_process_id,monitor_process_item_id,state,created_at,updated_at FROM course WHERE code = '"+id+"'"
    row = None
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL, sql)
    if (len(result) > 0):
        row = result[0]
    return row

def getCoursesByStudyGroupCode(study_group_code):
    sql = "SELECT code,uuid,name,fullname FROM course WHERE study_group_code = '"+study_group_code+"'"
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL, sql)
    return result