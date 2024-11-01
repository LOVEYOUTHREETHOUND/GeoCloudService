# import cx_Oracle
from flask import Flask
# import mapper_config 
# from DBUtils.PooledDB import PooledDB



# database = {
#     'user': 'jgf_gxfw',
#     'password' : 'icw3kx45',
#     'host' : '10.82.8.4',
#     'database' : 'jgf_gxfw',
#     'port' : '1521'
# }



# # 创建连接对象，连接底层Oracle数据库
# def create_dbconn():
#     db_config = database
#     username = db_config.get('user')
#     password = db_config.get('password')
#     host = db_config.get('host')
#     port = db_config.get('port')
#     database = db_config.get('database')
#     str = username + '/' + password + '@' + host + ':' + port + '/' + database
#     conn = cx_Oracle.connect(str)
#     return conn

# # 创建连接池，连接池底层Oracle数据库
# def create_pool():
#     db_config = database
#     username = db_config.get('user')
#     password = db_config.get('password')
#     host = db_config.get('host')
#     port = db_config.get('port')
#     database = db_config.get('database')
#     dsn = cx_Oracle.makedsn(host, port, database)
#     pool = PooledDB(cx_Oracle, user=username, password=password, dsn=dsn)
#     return pool












# 创建连接池，连接池底层Oracle数据库
# import cx_Oracle
import oracledb
from flask import Flask
# import dbconfig
# from dbutils.pooled_db import PooledDB

app = Flask(__name__)


# db_config = dbconfig.database
username = 'jgf_gxfw'
password = 'icw3kx45'
host = '10.82.8.4'
port = '1521'
database = 'jgf_gxfw'
str = username + '/' + password + '@' + host + ':' + port + '/' + database
# conn = oracledb.makedsn(host, port, service_name=database)
# conn = oracledb.connect(str)
# conn = oracledb.connect('jgf_gxfw/icw3kx45@10.82.8.4:1521/jgf_gxfw')


# 连接数据库
conn = oracledb.connect(user="jgf_gxfw",password="icw3kx45",dsn="10.82.8.4:1521/satdb")
# conn = oracledb.connect(user="JGF_GXFW",password="JGF_GXFW",dsn="62.234.192.247:18881/ORCLCDB")
# conn=cx_Oracle.connect('JGF_GXFW/JGF_GXFW@62.234.192.247:18881/ORCLCDB')


cursor=conn.cursor()


# 已完成订单总数统计
# cursor.execute("SELECT COUNT(*) FROM TF_ORDER WHERE F_STATUS NOT IN (-1,  2, 5) AND F_CREATTIME BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-09-30 23:59:59'")
# result=cursor.fetchall()
# print("1.已完成订单数量：")
# print(result)

#离线订单总数统计
cursor.execute("SELECT COUNT(*) FROM TF_ORDER WHERE F_STATUS  IN (5) AND F_CREATTIME BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-09-30 23:59:59' AND F_GET_METHOD = '线下拷贝'")
result=cursor.fetchone()
print("离线订单数量：")
print(result)

#在线订单总数统计
cursor.execute("SELECT COUNT(*) FROM TF_ORDER WHERE F_STATUS  IN (5) AND F_CREATTIME BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-09-30 23:59:59' AND F_GET_METHOD = '在线下载'")
result=cursor.fetchall()
print("在线订单数量：")
print(result)

# #4、已完成订单总数据量统计
# cursor.execute("SELECT F_DATA_SUM FROM TF_ORDER WHERE F_STATUS NOT IN (-1, 2, 5) AND F_CREATTIME BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-09-30 23:59:59'")
# results=cursor.fetchall()
# 初始化总和
# total_sum = 0
# 处理每个结果
# for result in results:
#     data_sum = result[0].strip()  # 去除空格
#     if data_sum.endswith('M'):
#         data_sum = data_sum[:-1]  # 去除 'M'
#         total_sum += float(data_sum)
#     elif data_sum.endswith('G'):
#         data_sum = data_sum[:-1]  # 去除 'G'
#         total_sum += float(data_sum) * 1024
#     else:
#         total_sum = total_sum
# print(results)
# # 打印总和
# print("总和：", total_sum)
# print("4.已完成订单总数据量：")
# print(result)

# #5、离线订单总数据量统计
cursor.execute("SELECT F_DATA_SUM FROM TF_ORDER WHERE F_STATUS IN (5) AND F_CREATTIME BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-09-30 23:59:59' AND F_GET_METHOD = '线下拷贝'")
results=cursor.fetchall()
# 初始化总和
total_sum = 0
# 处理每个结果
for result in results:
    data_sum = result[0].strip()  # 去除空格
    if data_sum.endswith('M'):
        data_sum = data_sum[:-1]  # 去除 'M'
        total_sum += float(data_sum)
    elif data_sum.endswith('G'):
        data_sum = data_sum[:-1]  # 去除 'G'
        total_sum += float(data_sum) * 1024
    else:
        total_sum = total_sum
total_sum=total_sum/1024/1024
# print(results)
# 打印总和
print("离线订单总数据量：", total_sum)


# #6、在线订单总数据量统计
cursor.execute("SELECT F_DATA_SUM FROM TF_ORDER WHERE F_STATUS IN (5) AND F_CREATTIME BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-09-30 23:59:59' AND F_GET_METHOD = '在线下载'")
results=cursor.fetchall()
# 初始化总和
total_sum = 0
# 处理每个结果
for result in results:
    data_sum = result[0].strip()  # 去除空格
    if data_sum.endswith('M'):
        data_sum = data_sum[:-1]  # 去除 'M'
        total_sum += float(data_sum)
    elif data_sum.endswith('G'):
        data_sum = data_sum[:-1]  # 去除 'G'
        total_sum += float(data_sum) * 1024
    else:
        total_sum = total_sum
total_sum=total_sum/1024/1024
# print(results)
# 打印总和
print("在线订单总数据量：", total_sum)



#7、已完成订单总景数统计
# cursor.execute("SELECT SUM(F_DATACOUNT) FROM TF_ORDER WHERE F_STATUS NOT IN (-1,  2,5) AND F_CREATTIME BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-09-30 23:59:59'")
# result=cursor.fetchall()
# print("7.已完成订单总景数：")
# print(result)

#8、离线订单总景数统计  
cursor.execute("SELECT SUM(F_DATACOUNT) FROM TF_ORDER WHERE F_STATUS IN (5) AND F_CREATTIME BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-09-30 23:59:59' AND F_GET_METHOD = '线下拷贝'")
result=cursor.fetchall()
print("离线订单总景数：",result)


#9、在线订单总景数统计
cursor.execute("SELECT SUM(F_DATACOUNT) FROM TF_ORDER WHERE F_STATUS IN (5) AND F_CREATTIME BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-09-30 23:59:59' AND F_GET_METHOD = '在线下载'")
result=cursor.fetchall()
print("在线订单总景数：",result)











