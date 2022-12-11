import dao.DBManager


def insert(params):
    sql = "INSERT INTO study_group(code,academic_period_id,campus_name,level_name,grade_name,section_name,monitor_process_id,monitor_process_item_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,DATETIME('now'),DATETIME('now'))"
    result = dao.DBManager.insertUpdateDelete(
        dao.DBManager.DB_LOCAL, sql, params)
    return result


def update(params):
    sql = "UPDATE monitor_process SET updated_at = DATETIME('now'),num_items=?,gas_monitor_process_id=?,gas_monitor_process_item_id=?,state = ? WHERE id = ?"
    result = dao.DBManager.insertUpdateDelete(
        dao.DBManager.DB_LOCAL, sql, params)
    return result


def findById(id):
    sql = "SELECT code,academic_period_id,campus_name,level_name,grade_name,section_name,monitor_process_id,monitor_process_item_id,created_at,updated_at FROM study_group WHERE code = '"+id+"'"
    row = None
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL, sql)
    if (len(result) > 0):
        row = result[0]
    return row
