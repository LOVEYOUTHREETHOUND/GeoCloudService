# import cx_Oracle
import oracledb
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
    conn = oracledb.connect(str)
    return conn

def create_pool():
    db_config = dbconfig.database
    pool_config = dbconfig.PoolDB
    username = db_config.get('user')
    password = db_config.get('password')
    host = db_config.get('host')
    port = db_config.get('port')
    database = db_config.get('database')
    maxcached = pool_config.get('maxcached')
    mincached = pool_config.get('mincached')
    maxshared = pool_config.get('maxshared')
    maxconnections = pool_config.get('maxconnections') 
    blocking = pool_config.get('blocking')
    # 若使用服务名连接数据库，使用下面的语句
    # dsn = oracledb.makedsn(host, port, service_name=database)
    # 若使用SID连接数据库，使用下面的语句
    dsn = oracledb.makedsn(host, port, sid=database)
    pool = PooledDB(oracledb, user=username, password=password, dsn=dsn,
                    mincached=mincached, maxcached=maxcached, maxshared=maxshared, 
                    maxconnections=maxconnections, blocking=blocking)
    return pool