import cx_Oracle
import dbconfig
from dbutils.pooled_db import PooledDB

def create_dbconn():
    db_config = dbconfig.database
    username = db_config.get('user')
    password = db_config.get('password')
    host = db_config.get('host')
    port = db_config.get('port')
    database = db_config.get('database')
    str = username + '/' + password + '@' + host + ':' + port + '/' + database
    conn = cx_Oracle.connect(str)
    return conn

def create_pool():
    db_config = dbconfig.database
    username = db_config.get('user')
    password = db_config.get('password')
    host = db_config.get('host')
    port = db_config.get('port')
    database = db_config.get('database')
    dsn = cx_Oracle.makedsn(host, port, database)
    pool = PooledDB(cx_Oracle, user=username, password=password, dsn=dsn)
    return pool