# import cx_Oracle
import oracledb
# import config.dbconfig as dbconfig
import src.config.config as config 
from dbutils.pooled_db import PooledDB

oracledb.init_oracle_client()

username = config.DB_USER
password = config.DB_PWD
host = config.DB_HOST
port = config.DB_PORT
database = config.DB_DATABASE
maxcached = config.DB_POOL_MAXCACHE
mincached = config.DB_POOL_MINCACHE
maxshared = config.DB_POOL_MAXSHARED
maxconnections = config.DB_POOL_MAXCONNECTIONS
blocking = config.DB_POOL_BLOCKING

def create_dbconn():
    str = username + '/' + password + '@' + host + ':' + port + '/' + database
    conn = oracledb.connect(str)
    return conn

def create_pool():
    # 若使用服务名连接数据库，使用下面的语句
    # dsn = oracledb.makedsn(host, port, service_name=database)
    # 若使用SID连接数据库，使用下面的语句
    dsn = oracledb.makedsn(host, port, sid=database)
    pool = PooledDB(oracledb, user=username, password=password, dsn=dsn,
                    mincached=mincached, maxcached=maxcached, maxshared=maxshared, 
                    maxconnections=maxconnections, blocking=blocking)
    return pool

Mypool = create_pool()