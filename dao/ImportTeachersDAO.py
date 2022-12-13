import dao.DBManager


def insert(params):
    sql = "INSERT INTO import_teachers(course_code,study_group_code,teacher_code,name,lastname,fullname,monitor_process_id,monitor_process_item_id,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,DATETIME('now'),DATETIME('now'))"
    result = dao.DBManager.insertUpdateDelete(
        dao.DBManager.DB_LOCAL, sql, params)
    return result

def getTeachers(monitor_process_id,monitor_process_item_id):
    sql = "SELECT teacher_code,name,lastname,fullname,state,created_at,updated_at FROM import_teachers "
    sql += "WHERE monitor_process_id = '"+str(monitor_process_id)+"' and monitor_process_item_id = '"+str(monitor_process_item_id)+"' GROUP BY teacher_code"
    # sql += " GROUP BY teacher_code"
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL, sql)
    return result

def getCoursesByTeachers(teacher_code):
    sql = "SELECT it.study_group_code,it.course_code,it.state,it.created_at,it.updated_at, c.name as course_name, c.fullname as course_fullname "
    sql += "FROM import_teachers it INNER JOIN course c ON(it.course_code = c.code) "
    sql += "WHERE it.teacher_code = '"+teacher_code+"' GROUP BY it.study_group_code,it.course_code"
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL, sql)
    return result    


def findIfExists(course_code,study_group_code,teacher_code):
    sql = "SELECT id,course_code,study_group_code,teacher_code,monitor_process_id,monitor_process_item_id,state,created_at,updated_at FROM import_teachers "
    sql += "WHERE course_code = '"+course_code+"' and study_group_code = '"+study_group_code+"' and teacher_code = '"+teacher_code+"'"
    row = None
    result = dao.DBManager.executeResult(dao.DBManager.DB_LOCAL, sql)
    if (len(result) > 0):
        row = result[0]
    return row
