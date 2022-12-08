import sys
import os
import sqlite3
import json
import config
import helpers
from sqlite3 import Error

file_params_pubsub = open(os.path.dirname(
    os.path.abspath(__file__).replace("/dao", ""))+"/config.json")
pubsubObject = json.load(file_params_pubsub)

DB_LOCAL = os.path.dirname(os.path.abspath(__file__))+'localDB.sqlite3'
DB_KOLIBRI = pubsubObject["DB_PATH_KOLIBRI"]

def executeResult(DB_PATH, sql):
    conn = None
    list_data = []
    try:
        conn = sqlite3.connect(DB_PATH, uri=True)
        result = conn.execute(sql)
        for row in result:
            # {"channel_id": row[0], "channel_name": row[1], "title": row[2], "kind": row[3], "file_size": row[4]})
            list_data.append(row)
        return list_data
    except Error as e:
        print(e)
        list_data = []
    finally:
        if conn:
            conn.close()
        return list_data

def insertUpdateDelete(DB_PATH, sql, params):
    is_registered = True
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, uri=True)
        cursor = conn.cursor()
        cursor.execute(sql, params)
        conn.commit()
        cursor.close()       
    except Error as e:
        print(e)
        is_registered = False
    finally:
        if conn:
            conn.close()
    return is_registered

def createTablesIfNotExists():
    conn = None    
    try:
        conn = sqlite3.connect(DB_LOCAL, uri=True)

        sql = "CREATE TABLE IF NOT EXISTS monitor_process( id INTEGER NOT NULL PRIMARY KEY  AUTOINCREMENT, command VARCHAR(255),action VARCHAR(255),mode VARCHAR(255), num_items INTEGER,state VARCHAR(255),runned_at DATETIME,updated_at DATETIME)"
        conn.execute(sql)

        sql = "CREATE TABLE IF NOT EXISTS monitor_process_item( id INTEGER NOT NULL PRIMARY KEY  AUTOINCREMENT,monitor_process_id INTEGER,num_items INTEGER,state VARCHAR(255),created_at DATETIME,updated_at DATETIME)"
        conn.execute(sql)

        sql = "CREATE TABLE IF NOT EXISTS academic_period( code VARCHAR(255) NOT NULL PRIMARY KEY,name VARCHAR(255),domain VARCHAR(255),school_id VARCHAR(255),state VARCHAR(255),created_at DATETIME,updated_at DATETIME,monitor_process_id INTEGER,monitor_process_item_id INTEGER)"
        conn.execute(sql)

        sql = "CREATE TABLE IF NOT EXISTS study_group( code VARCHAR(255) NOT NULL PRIMARY KEY,academic_period_id VARCHAR(255),campus_name VARCHAR(255),level_name VARCHAR(255),grade_name VARCHAR(255),section_name VARCHAR(255),created_at DATETIME,updated_at DATETIME,monitor_process_id INTEGER,monitor_process_item_id INTEGER)"
        conn.execute(sql)

        sql = "CREATE TABLE IF NOT EXISTS courses( code VARCHAR(255) NOT NULL PRIMARY KEY,name VARCHAR(255),academic_period_id VARCHAR(255),study_group_code VARCHAR(255),campus_name VARCHAR(255),level_name VARCHAR(255),grade_name VARCHAR(255),section_name VARCHAR(255),course_code VARCHAR(255),course_name VARCHAR(255),state VARCHAR(255),created_at DATETIME,updated_at DATETIME,monitor_process_id INTEGER,monitor_process_item_id INTEGER)"
        conn.execute(sql)

        sql = "CREATE TABLE IF NOT EXISTS students( code VARCHAR(255) NOT NULL PRIMARY KEY,name VARCHAR(255),academic_period_id VARCHAR(255),study_group_code VARCHAR(255),campus_name VARCHAR(255),level_name VARCHAR(255),grade_name VARCHAR(255),section_name VARCHAR(255),course_code VARCHAR(255),course_name VARCHAR(255),state VARCHAR(255),created_at DATETIME,updated_at DATETIME,monitor_process_id INTEGER,monitor_process_item_id INTEGER)"
        conn.execute(sql)

        # sql = "CREATE TABLE IF NOT EXISTS local_people( id INTEGER NOT NULL PRIMARY KEY, gid VARCHAR(255), name VARCHAR(255), lastname VARCHAR(255),username VARCHAR(255), email VARCHAR(255), state VARCHAR(255), created_at DATETIME, updated_at DATETIME, deleted_at DATETIME, local_organization_id INTEGER )"
        # conn.execute(sql)
        
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()