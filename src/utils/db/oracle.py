import oracledb
import src.config.config as config 
from src.utils.logger import logger

# #oracledb.init_oracle_client()

username = config.DB_USER
password = config.DB_PWD
host = config.DB_HOST
port = config.DB_PORT
database = config.DB_DATABASE
max = config.DB_POOL_MAX
min = config.DB_POOL_MIN
increment = config.DB_POOL_INCREMENT


def create_dbconn():
    str = username + '/' + password + '@' + host + ':' + port + '/' + database
    conn = oracledb.connect(str)
    return conn

def create_pool():
    # 若使用服务名连接数据库，使用下面的语句
    dsn = oracledb.makedsn(host, port, service_name=database)
    # 若使用SID连接数据库，使用下面的语句
    # dsn = oracledb.makedsn(host, port, sid=database)
    pool = oracledb.create_pool(user=username, password=password, dsn=dsn,
                    min=min, max=max, increment=increment)
    return pool

def executeQuery(pool: oracledb.ConnectionPool, sql: str, params = None):
    try:
        with pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                res = cur.fetchall()
        return res
    except Exception as e:
        logger.error(f'执行SQL语句失败: {e}, sql: {sql}, params: {params}')
        return None
    
def executeNonQuery(pool: oracledb.ConnectionPool, sql: str, params = None):
    try:
        with pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                conn.commit()   
    except Exception as e:
        logger.error(f'执行SQL语句失败: {e}, sql: {sql}, params: {params}')
 