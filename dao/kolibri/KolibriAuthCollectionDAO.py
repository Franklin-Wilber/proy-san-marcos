import dao.DBManager
import sqlite3
from sqlite3 import Error

def findByName(name):
    sql = "SELECT * FROM kolibriauth_collection where name = '"+name+"'"
    course  = None
    result = dao.DBManager.executeResult(dao.DBManager.DB_KOLIBRI,sql)
    if( len(result) > 0 ):
        course = result[0]
    return course

def findById(id):
    sql = "SELECT * FROM kolibriauth_collection where id = '"+id+"'"
    course  = None
    result = dao.DBManager.executeResult(dao.DBManager.DB_KOLIBRI,sql)
    if( len(result) > 0 ):
        course = result[0]
    return course    

def listCollections():
    list_data = []
    sql = "SELECT * FROM kolibriauth_collection"
    result = dao.DBManager.executeResult(dao.DBManager.DB_KOLIBRI,sql)       
    for row in result:
        list_data.append({ "id": row[0],"name": row[4],"kind": row[5] })
    return list_data

def insert(c):
    params = (c["id"],c["name"],c["kind"],c["dataset_id"],c["parent_id"],c["_morango_dirty_bit"],c["_morango_source_id"],c["_morango_partition"] )
    # print(params)
    sql = "INSERT INTO kolibriauth_collection(id,name,kind,dataset_id,parent_id,_morango_dirty_bit,_morango_source_id,_morango_partition) VALUES(?,?,?,?,?,?,?,?)"
    b = dao.DBManager.insertUpdateDelete(dao.DBManager.DB_KOLIBRI,sql,params)
    return b