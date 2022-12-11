import dao.DBManager

def sync(user,action):
    b = False
    if(action == 'INSERT'):
        sql = "INSERT INTO kolibriauth_facilityuser(id,last_login,username,password,full_name,date_joined,birth_year,gender,dataset_id,facility_id,id_number,_morango_dirty_bit,_morango_source_id,_morango_partition) "
        sql = sql +"VALUES(?,?,?,?,?,DATE('now'),?,?,?,?,?,?,?,?)"
        params = ( user["id"] ,user["last_login"],
                    user["username"],user["password"],
                    user["full_name"],
                    user["birth_year"],user["gender"],
                    user["dataset_id"],user["facility_id"],
                    user["id_number"],user["_morango_dirty_bit"],
                    user["_morango_source_id"],user["_morango_partition"]
                )
        b = dao.DBManager.insertUpdateDelete(dao.DBManager.DB_KOLIBRI,sql,params)
    if(action == 'UPDATE'):
        # sql = "UPDATE local_user SET name=?,state=?,updated_at=? WHERE id = ?"
        # params = ( user["name"],user["state"],user["updated_at"], user["id"] )
        # b = dao.DB.execute(sql,params)   
        b = False 
    return b

def findByUsername(username):
    sql = "SELECT id,last_login,username,password,full_name,date_joined,birth_year,gender,dataset_id,facility_id,id_number,_morango_dirty_bit,_morango_source_id,_morango_partition FROM kolibriauth_facilityuser WHERE username = '"+username+"'"
    row = None
    result = dao.DBManager.executeResult(dao.DBManager.DB_KOLIBRI,sql)
    if (len(result) > 0):
        row = result[0]
    return row