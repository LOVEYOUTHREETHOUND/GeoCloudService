# from geocloudservice.db import oracle
import utils.db.oracle as oracle
import config.mapper_config as mapper_config 

class Mapper:
    def __init__(self, pool):
        self.pool = pool
        
    # 从TF_ORDER里面查询最近20条未处理的订单ID和订单名
    def getIdByStatus(self):
        config = mapper_config.mapconfig
        count = config.get("process_count")
        # pool = oracle.create_pool()
        conn = self.pool.connection()
        cursor = conn.cursor()
        sql = "SELECT F_ID, F_ORDERNAME FROM (SELECT F_ID, F_ORDERNAME FROM TF_ORDER WHERE F_STATUS = 1 \
                AND F_ORDERNAME IS NOT NULL ORDER BY F_ORDERNAME DESC) WHERE ROWNUM <= {}".format(count)
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result

    #根据订单ID从TF_ORDERDATA里面查询订阅数据名 
    def getDatanameByOrderId(self,f_orderid):
        # pool = oracle.create_pool()
        conn = self.pool.connection()
        cursor = conn.cursor()
        sql = "SELECT F_DATANAME FROM TF_ORDERDATA WHERE F_ORDERID = {} AND F_STATUS = 1".format(f_orderid)
        # print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result

    # 根据订单名在TF_ORDER中更新订单状态
    def updateOrderStatusByOrdername(self,f_ordername):
        # pool = oracle.create_pool()
        conn = self.pool.connection()
        cursor = conn.cursor()
        sql = "UPDATE TF_ORDER SET F_STATUS = 6 WHERE F_ORDERNAME = '{}'".format(f_ordername)
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()

    # 根据订阅数据名和订单ID在TF_ORDERDATA中更新订阅数据状态
    def updateDataStatusByNameAndId(self,f_dataname, f_orderid):
        # pool = oracle.create_pool()
        conn = self.pool.connection()
        cursor = conn.cursor()
        sql = "UPDATE TF_ORDERDATA SET F_STATUS = 0 WHERE F_DATANAME = '{}' AND F_ORDERID = '{}'".format(f_dataname, f_orderid)
        # print(sql)
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()
        
    # 根据订单名获取订单ID(f_orderid)
    def getIdByOrdername(self,f_ordername):
        # pool = oracle.create_pool()
        conn = self.pool.connection()
        cursor = conn.cursor()
        sql = "SELECT F_ID FROM TF_ORDER WHERE F_ORDERNAME = '{}'".format(f_ordername)
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result[0][0]

    # 根据订单ID获取未完成的订单数量
    def getCountByOrderId(self,f_orderid):
        conn = self.pool.connection()
        cursor = conn.cursor()
        sql = "SELECT COUNT(*) FROM TF_ORDERDATA WHERE F_ORDERID = {} AND F_STATUS = 1".format(f_orderid)
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result[0][0]

    # 根据订单ID从TF_ORDER中获取所有信息
    # 返回格式为列名：数据
    def getAllByOrderIdFromOrder(self,f_orderid):
        conn = self.pool.connection()
        cursor = conn.cursor()
        sql = "SELECT * FROM TF_ORDER WHERE F_ID = {}".format(f_orderid)
        cursor.execute(sql)
        columns = [col[0] for col in cursor.description]
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        
        data = [dict(zip(columns, row)) for row in result]
        return data
    
    # 根据订单ID和数据名从TF_ORDERDATA中获取所有信息
    # 返回格式为列名：数据
    def getAllByOrderIdFromOrderData(self,f_orderid,f_dataname):
        conn = self.pool.connection()
        cursor = conn.cursor()
        sql = "SELECT * FROM TF_ORDERDATA \
            WHERE F_ORDERID = {} AND F_DATANAME = '{}'".format(f_orderid,f_dataname)
        # print(sql)
        cursor.execute(sql)
        columns = [col[0] for col in cursor.description]
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        
        data = [dict(zip(columns, row)) for row in result]
        return data

    # 向数据库中插入数据以创建Serv-U用户
    # 向FTP_SUUSERS表中插入数据以创建用户
    # 向FTP_USERDIRACCESS表中插入数据以配置用户权限
    def insertServUInfo(self, starttime, endtime, ordername, pwd):
        RtDailyCount = "0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            
            sql1 = """
            INSERT INTO FTP_SUUSERS (
                "StatisticsStartTime", "RtServerStartTime", "RtDailyCount", "LoginID", 
                "PasswordChangedOn", "PasswordEncryptMode", "PasswordUTF8", "Password", 
                "Type", "ExpiresOn", "HomeDir", "IncludeRespCodesInMsgFiles", 
                "ODBCVersion", "Quota") 
                VALUES (
                :starttime, :starttime, :RtDailyCount, :ordername, 
                :endtime, '1', '1', :pwd, 
                '2', :endtime, 'Z:\\shareJGF\\order\\data\\' || :ordername, 
                '1', '4', '0'
            )
            """
            # print("Executing SQL 1:", sql1)
            
            sql2 = """
            INSERT INTO FTP_USERDIRACCESS (
                "LoginID", "SortIndex", "Dir", "Access"
            ) VALUES (
                :ordername, 1, 'Z:\\shareJGF\\order\\data\\' || :ordername, '4383'
            )
            """
            # print("Executing SQL 2:", sql2)
            
            cursor.execute(sql1, {
                'starttime': starttime,
                'RtDailyCount': RtDailyCount,
                'ordername': ordername,
                'endtime': endtime,
                'pwd': pwd
            })
            print("SQL 1 executed successfully")
            
            cursor.execute(sql2, {
                'ordername': ordername
            })
            print("SQL 2 executed successfully")
            
            conn.commit()
            print("Serv-U user created successfully")
        except Exception as e:
            print("Error executing SQL:", e)
        finally:
            cursor.close()
            conn.close()
        
    # 向TF_ORDER表中对应用户插入密码
    def insertServUPwd(self, ordername, pwd):
        conn = self.pool.connection()
        cursor = conn.cursor()
        
        sql = "UPDATE TF_ORDER SET F_PASSWORD = '{}' WHERE F_ORDERNAME = '{}'".format(pwd, ordername)
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()