import dao.DBManager


def sync(user, action):
    b = False
    if (action == 'INSERT'):
        sql = "INSERT INTO kolibriauth_facilityuser(id,last_login,username,password,full_name,date_joined,birth_year,gender,dataset_id,facility_id,id_number,_morango_dirty_bit,_morango_source_id,_morango_partition) "
        sql = sql + "VALUES(?,?,?,?,?,DATE('now'),?,?,?,?,?,?,?,?)"
        params = (user["id"], user["last_login"],
                  user["username"], user["password"],
                  user["full_name"],
                  user["birth_year"], user["gender"],
                  user["dataset_id"], user["facility_id"],
                  user["id_number"], user["_morango_dirty_bit"],
                  user["_morango_source_id"], user["_morango_partition"]
                  )
        b = dao.DBManager.insertUpdateDelete(
            dao.DBManager.DB_KOLIBRI, sql, params)
    return b


def getSuperAdmin():
    superAdmin = None
    sql = "SELECT fu.id,fu.last_login,fu.username,fu.password,fu.full_name,fu.date_joined,fu.birth_year,fu.gender,fu.dataset_id,fu.facility_id,fu.id_number,fu._morango_dirty_bit,fu._morango_source_id,fu._morango_partition FROM kolibriauth_facilityuser fu INNER JOIN kolibriauth_role kr ON(fu.id = kr.user_id) WHERE kr.kind = 'admin'"
    result = dao.DBManager.executeResult(dao.DBManager.DB_KOLIBRI, sql)
    if (len(result) > 0):
        row = result[0]
        superAdmin = {
            "id": row[0],
            "last_login": row[1],
            "username": row[2],
            "password": row[3],
            "full_name": row[4],
            "date_joined": row[5],
            "birth_year": row[6],
            "gender": row[7],
            "dataset_id": row[8],
            "facility_id": row[9],
            "id_number": row[10],
            "_morango_dirty_bit": row[11],
            "_morango_source_id": row[12],
            "_morango_partition": row[13]
        }
    return superAdmin


def findByUsername(username):
    sql = "SELECT fu.id,fu.last_login,fu.username,fu.password,fu.full_name,fu.date_joined,fu.birth_year,fu.gender,fu.dataset_id,fu.facility_id,fu.id_number,fu._morango_dirty_bit,fu._morango_source_id,fu._morango_partition FROM kolibriauth_facilityuser fu WHERE fu.username = '"+username+"'"
    user = None
    result = dao.DBManager.executeResult(dao.DBManager.DB_KOLIBRI, sql)
    if (len(result) > 0):
        row = result[0]
        user = {
            "id": row[0],
            "last_login": row[1],
            "username": row[2],
            "password": row[3],
            "full_name": row[4],
            "date_joined": row[5],
            "birth_year": row[6],
            "gender": row[7],
            "dataset_id": row[8],
            "facility_id": row[9],
            "id_number": row[10],
            "_morango_dirty_bit": row[11],
            "_morango_source_id": row[12],
            "_morango_partition": row[13]
        }
    return user

def findById(id):
    sql = "SELECT fu.id,fu.last_login,fu.username,fu.password,fu.full_name,fu.date_joined,fu.birth_year,fu.gender,fu.dataset_id,fu.facility_id,fu.id_number,fu._morango_dirty_bit,fu._morango_source_id,fu._morango_partition FROM kolibriauth_facilityuser fu WHERE fu.id = '"+id+"'"
    user = None
    result = dao.DBManager.executeResult(dao.DBManager.DB_KOLIBRI, sql)
    if (len(result) > 0):
        row = result[0]
        user = {
            "id": row[0],
            "last_login": row[1],
            "username": row[2],
            "password": row[3],
            "full_name": row[4],
            "date_joined": row[5],
            "birth_year": row[6],
            "gender": row[7],
            "dataset_id": row[8],
            "facility_id": row[9],
            "id_number": row[10],
            "_morango_dirty_bit": row[11],
            "_morango_source_id": row[12],
            "_morango_partition": row[13]
        }
    return user


def listUsers():
    sql = "SELECT id,last_login,username,password,full_name,date_joined,birth_year,gender,dataset_id,facility_id,id_number,_morango_dirty_bit,_morango_source_id,_morango_partition FROM kolibriauth_facilityuser"
    list_users = []
    result = dao.DBManager.executeResult(dao.DBManager.DB_KOLIBRI, sql)
    if (len(result) > 0):
        row = result[0]
        for row in result:
            list_users.append({
                "id": row[0],
                "last_login": row[1],
                "username": row[2],
                "password": row[3],
                "full_name": row[4],
                "date_joined": row[5],
                "birth_year": row[6],
                "gender": row[7],
                "dataset_id": row[8],
                "facility_id": row[9],
                "id_number": row[10],
                "_morango_dirty_bit": row[11],
                "_morango_source_id": row[12],
                "_morango_partition": row[13]
            })
    return list_users
