# from geocloudservice.db import oracle
# from src.utils.db.oracle import Mypool 
from src.utils.db.oracle import create_pool
import src.config.mapper_config as mapper_config 
import src.utils.logger as logger

class Mapper:
    def __init__(self):
        self.pool = create_pool()
        
    # def __init__(self,pool):
    #     self.pool = pool
        
    # 从TF_ORDER里面查询最近20条未处理的订单ID和订单名
    def getIdByStatus(self):
        try:
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
        except Exception as e:
            logger.error("获取订单ID错误: %s" % e)
            return []

    #根据订单ID从TF_ORDERDATA里面查询订阅数据名 
    def getDatanameByOrderId(self,f_orderid):
        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            sql = "SELECT F_DATANAME FROM TF_ORDERDATA WHERE F_ORDERID = {} AND F_STATUS = 1".format(f_orderid)
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()
            conn.close()
            return result
        except Exception as e:
            logger.error("获取订阅数据名错误: %s" % e)
            return []

    # 根据订单名在TF_ORDER中更新订单状态
    def updateOrderStatusByOrdername(self,f_ordername):
        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            sql = "UPDATE TF_ORDER SET F_STATUS = 6 WHERE F_ORDERNAME = '{}'".format(f_ordername)
            cursor.execute(sql)
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            logger.error("更新订单状态错误: %s" % e)

    # 根据订阅数据名和订单ID在TF_ORDERDATA中更新订阅数据状态
    def updateDataStatusByNameAndId(self,f_dataname, f_orderid):
        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            sql = "UPDATE TF_ORDERDATA SET F_STATUS = 0 WHERE F_DATANAME = '{}' AND F_ORDERID = '{}'".format(f_dataname, f_orderid)
            cursor.execute(sql)
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            logger.error("更新订单数据状态错误: %s" % e)
        
    # 根据订单名获取订单ID(f_orderid)
    def getIdByOrdername(self,f_ordername):
        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            sql = "SELECT F_ID FROM TF_ORDER WHERE F_ORDERNAME = '{}'".format(f_ordername)
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()
            conn.close()
            return result[0][0]
        except Exception as e:
            logger.error("获取订单ID错误: %s" % e)
            return 0

    # 根据订单ID获取未完成的订单数量
    def getCountByOrderId(self,f_orderid):
        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            sql = "SELECT COUNT(*) FROM TF_ORDERDATA WHERE F_ORDERID = {} AND F_STATUS = 1".format(f_orderid)
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()
            conn.close()
            return result[0][0]
        except Exception as e:
            logger.error("获取订单数量错误: %s" % e)
            return 0

    # 根据订单ID从TF_ORDER中获取所有信息
    # 返回格式为列名：数据
    def getAllByOrderIdFromOrder(self,f_orderid):
        try:
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
        except Exception as e:
            logger.error("获取订单信息错误: %s" % e)
            return data
    
    # 根据订单ID和数据名从TF_ORDERDATA中获取所有信息
    # 返回格式为列名：数据
    def getAllByOrderIdFromOrderData(self,f_orderid,f_dataname):
        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            sql = "SELECT * FROM TF_ORDERDATA \
                WHERE F_ORDERID = {} AND F_DATANAME = '{}'".format(f_orderid,f_dataname)
            cursor.execute(sql)
            columns = [col[0] for col in cursor.description]
            result = cursor.fetchall()
            cursor.close()
            conn.close()
            
            data = [dict(zip(columns, row)) for row in result]
            return data
        except Exception as e:
            logger.error("获取订单数据错误: %s" % e)
            return data

    # 向数据库中插入数据以创建Serv-U用户
    # 向FTP_SUUSERS表中插入数据以创建用户
    # 向FTP_USERDIRACCESS表中插入数据以配置用户权限
    def insertServUInfo(self, starttime ,endtime, ordername, pwd):
        RtDailyCount = "0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            
            sql1 = """
                MERGE INTO FTP_SUUSERS t
                USING (SELECT :ordername AS LoginID FROM dual) d
                ON (t."LoginID" = d.LoginID)
                WHEN NOT MATCHED THEN
                INSERT (
                    "StatisticsStartTime", "RtServerStartTime", "RtDailyCount", "LoginID", 
                    "PasswordChangedOn", "PasswordEncryptMode", "PasswordUTF8", "Password", 
                    "Type", "ExpiresOn", "HomeDir", "IncludeRespCodesInMsgFiles", 
                    "ODBCVersion", "Quota"
                ) 
                VALUES (
                    :starttime, :starttime, :RtDailyCount, :ordername, 
                    :endtime, '1', '1', :pwd, 
                    '2', :endtime, 'Z:\\shareJGF\\order\\data\\' || :ordername, 
                    '1', '4', '0'
                )
                """
            # print("Executing SQL 1:", sql1)
            cursor.execute(sql1, {
                'starttime': starttime,
                'RtDailyCount': RtDailyCount,
                'ordername': ordername,
                'endtime': endtime,
                'pwd': pwd
            })
            sql2 = """
                MERGE INTO FTP_USERDIRACCESS t
                USING (SELECT :ordername AS LoginID FROM dual) d
                ON (t."LoginID" = d.LoginID)
                WHEN NOT MATCHED THEN
                INSERT (
                    "LoginID", "SortIndex", "Dir", "Access"
                ) 
                VALUES (
                    :ordername, 1, 'Z:\\shareJGF\\order\\data\\' || :ordername, '4383'
                )
                """
            
            # print("SQL 1 executed successfully")
            
            cursor.execute(sql2, {
                'ordername': ordername
            })
            # print("SQL 2 executed successfully")
            conn.commit()
        except Exception as e:
            logger.error("Serv-U用户创建错误: %s" % e)
        finally:
            cursor.close()
            conn.close()
        
    # 向TF_ORDER表中对应用户插入密码
    def insertServUPwd(self, ordername, pwd, md5):
        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            
            sql1 = "UPDATE TF_ORDER SET F_PASSWORD = '{}' WHERE F_ORDERNAME = '{}'".format(pwd, ordername)
            cursor.execute(sql1)
            # 保证TF_ORDER表中的密码和FTP_SUUSERS表中的密码一致
            sql2 = "UPDATE FTP_SUUSERS SET \"Password\" = '{}' WHERE \"LoginID\" = '{}'".format(md5, ordername)
            cursor.execute(sql2)
            conn.commit()
        except Exception as e:
            logger.error("Serv-U密码插入错误: %s" % e)
        finally:
            cursor.close()
            conn.close()
     
    # 从TF_ORDER表中查询测试订单
    def getTestOrder(self):
        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            sql = "SELECT * \
                    FROM TF_ORDER \
                    WHERE F_PRODUCT_NAME LIKE '%测试%' \
                    OR F_PRODUCT_NAME LIKE '%test%' \
                    OR F_PRODUCT_NAME LIKE '%Test%';"
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()
            conn.close()
            return result
        except Exception as e:
            logger.error("查询测试订单错误: %s" % e)
            return []

    # 将文件信息插入TF_ORDERDATA表中
    def insertOrderData(self,data):
        try:
            orderId = data.get("F_ID")
            # logger.info("正在插入订单数据{}".format(orderId))
            conn = self.pool.connection()
            cursor = conn.cursor()
            sql = """
            MERGE INTO TF_ORDERDATA t
            USING (SELECT :F_ID AS F_ID FROM dual) d
            ON (t.F_ID = d.F_ID)
            WHEN NOT MATCHED THEN
            INSERT (
                F_ID, F_ORDERID, F_DATANAME, F_SATELITE, F_SENSOR, F_RECEIVETIME, F_DATASIZE, 
                F_DATASOURCE, F_STATUS, F_DATAPATH, F_TASKID, F_DATATYPE, F_NODEID, F_DOCNUM, 
                F_DATAID, F_TM, F_FEEDBACK_CUSTOM_STATUS, F_FEEDBACK_OTHER_REQUEST, 
                F_FEEDBACK_TREAT_TIME, F_WKTRESPONSE, F_PRODUCTLEVEL, F_DOCNUM_OLD, F_NODENAME, 
                F_SGTABLENAME, F_DID, F_PUSH_STATUS, F_PUSH_START, F_PUSH_FINISH, 
                F_TRANSFER_STATUS, F_ORDER_TASK_ID, F_TRANSFER_COUNT, F_RECEIVE_STATUS, 
                F_PRODUCTID, F_SCENEID, F_CLOUDPERCENT, F_ORDER, F_ORBITID, F_SCENEPATH, 
                F_SCENEROW, F_ISASK, F_LOG, F_SYNC, F_SENDMQ
            )
            VALUES (
                :F_ID, :F_ORDERID, :F_DATANAME, :F_SATELITE, :F_SENSOR, TO_DATE(:F_RECEIVETIME, 'YYYY-MM-DD"T"HH24:MI:SS'), :F_DATASIZE, 
                :F_DATASOURCE, :F_STATUS, :F_DATAPATH, :F_TASKID, :F_DATATYPE, :F_NODEID, :F_DOCNUM, 
                :F_DATAID, :F_TM, :F_FEEDBACK_CUSTOM_STATUS, :F_FEEDBACK_OTHER_REQUEST, 
                :F_FEEDBACK_TREAT_TIME, :F_WKTRESPONSE, :F_PRODUCTLEVEL, :F_DOCNUM_OLD, :F_NODENAME, 
                :F_SGTABLENAME, :F_DID, :F_PUSH_STATUS, :F_PUSH_START, :F_PUSH_FINISH, 
                :F_TRANSFER_STATUS, :F_ORDER_TASK_ID, :F_TRANSFER_COUNT, :F_RECEIVE_STATUS, 
                :F_PRODUCTID, :F_SCENEID, :F_CLOUDPERCENT, :F_ORDER, :F_ORBITID, :F_SCENEPATH, 
                :F_SCENEROW, :F_ISASK, :F_LOG, :F_SYNC, :F_SENDMQ
            )
            """
            cursor.execute(sql, data)
            conn.commit()
            logger.info("订单数据{}插入成功".format(orderId))
        except Exception as e:
            logger.error("订单数据{}插入错误:{}".format(orderId, e))
        finally:
            cursor.close()
            conn.close()
            
    # 将文件信息插入TF_ORDER表中
    def insertOrder(self,data):
        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            sql = """
            MERGE INTO TF_ORDER t
            USING (SELECT :F_ORDERNAME AS F_ORDERNAME FROM dual) d
            ON (t.F_ORDERNAME = d.F_ORDERNAME)
            WHEN NOT MATCHED THEN
            INSERT (F_ID, F_ORDERNAME, F_ORDERCODE, F_CREATTIME, F_UPDATETIME, F_USERID, F_DISTFREQUENCY, 
                    F_STARTTIME, F_ENDTIME, F_STATUS, F_DISTMETHOD, F_TYPE, F_DESCRIPTION, F_PATHRULE, 
                    F_QUERY, F_DELAYTIME, F_SITENAME, F_ISCREATED, F_LEVEL, F_APPLYUSER, F_APPLYUSERPHONE, 
                    F_APPLYUSERUSED, F_APPLYUSERUNIT, F_DATATYPE, F_LEFTUPLONGITUDE, F_LEFTUPIMENSION, 
                    F_RIGHTDOWNLONGITUDE, F_RIGHTDOWNIMENSION, F_SPACETYPE, F_COUNTRYSPACE, F_PROVINCESPACE, 
                    F_CITYSPACE, F_TOWNSSPACE, F_SHPPATH, F_SATELLITE, F_SENSOR, F_CLOUDAMOUNT, F_SATLEVEL, 
                    F_USER_CARDID, F_GET_METHOD, F_PRODUCT_NAME, F_DATA_SUM, F_EXPECTED_APPLICATION_EFFECT, 
                    F_LOGIN_USER, DOWNLOD_PATH_FILE, F_CAUSE, F_PUSH_ID, F_DATA_TYPE_ID, F_GEOMETRY_ID, 
                    F_EXECUTE_TIME, F_TASK_STATUS, F_ORDER, F_PROCESS_DESCRIBE, F_ASSIGNMENT, F_DATACOUNT, 
                    F_SYSTEMTYPE, F_JDDM, F_TYFILEDOWN, F_PASSWORD, F_TYORDERID, F_TYOTHERINFO, F_ORDERLOG, 
                    F_TALLYGAG, F_NDWAY, F_ORDER_STATUS, F_RESPONSESPEED, F_SERVICEATTITUDE, F_FEEDBACKUPLOAD, 
                    F_MODIFYTYPE, F_SUBASSIGNMENT, F_EXTRACTINGELEMENTS, F_FEEDBACK, F_APPRAISE, F_SYNC, 
                    F_AUDITOR, F_DATASIZEKB, F_REPORTED)
            VALUES (:F_ID, :F_ORDERNAME, :F_ORDERCODE, TO_TIMESTAMP(:F_CREATTIME, 'YYYY-MM-DD"T"HH24:MI:SS.FF6'), 
                    TO_TIMESTAMP(:F_UPDATETIME,'YYYY-MM-DD"T"HH24:MI:SS.FF6'),
                    :F_USERID, :F_DISTFREQUENCY, 
                    :F_STARTTIME, :F_ENDTIME, :F_STATUS, :F_DISTMETHOD, :F_TYPE, :F_DESCRIPTION, :F_PATHRULE, 
                    :F_QUERY, :F_DELAYTIME, :F_SITENAME, :F_ISCREATED, :F_LEVEL, :F_APPLYUSER, :F_APPLYUSERPHONE, 
                    :F_APPLYUSERUSED, :F_APPLYUSERUNIT, :F_DATATYPE, :F_LEFTUPLONGITUDE, :F_LEFTUPIMENSION, 
                    :F_RIGHTDOWNLONGITUDE, :F_RIGHTDOWNIMENSION, :F_SPACETYPE, :F_COUNTRYSPACE, :F_PROVINCESPACE, 
                    :F_CITYSPACE, :F_TOWNSSPACE, :F_SHPPATH, :F_SATELLITE, :F_SENSOR, :F_CLOUDAMOUNT, :F_SATLEVEL, 
                    :F_USER_CARDID, :F_GET_METHOD, :F_PRODUCT_NAME, :F_DATA_SUM, :F_EXPECTED_APPLICATION_EFFECT, 
                    :F_LOGIN_USER, :DOWNLOD_PATH_FILE, :F_CAUSE, :F_PUSH_ID, :F_DATA_TYPE_ID, :F_GEOMETRY_ID, 
                    :F_EXECUTE_TIME, :F_TASK_STATUS, :F_ORDER, :F_PROCESS_DESCRIBE, :F_ASSIGNMENT, :F_DATACOUNT, 
                    :F_SYSTEMTYPE, :F_JDDM, :F_TYFILEDOWN, :F_PASSWORD, :F_TYORDERID, :F_TYOTHERINFO, :F_ORDERLOG, 
                    :F_TALLYGAG, :F_NDWAY, :F_ORDER_STATUS, :F_RESPONSESPEED, :F_SERVICEATTITUDE, :F_FEEDBACKUPLOAD, 
                    :F_MODIFYTYPE, :F_SUBASSIGNMENT, :F_EXTRACTINGELEMENTS, :F_FEEDBACK, :F_APPRAISE, :F_SYNC, 
                    :F_AUDITOR, :F_DATASIZEKB, :F_REPORTED)
        """
            cursor.execute(sql, data)
            conn.commit()
            ordername = data.get("F_ORDERNAME")
            logger.info("订单{}插入成功".format(ordername))
        except Exception as e:
            logger.error("订单{}插入错误: {}".format(ordername, e))
        finally:
            cursor.close()
            conn.close()
     
    # 根据用户Id获取用户邮箱 
    def getEmailByUserId(self,UserId):
        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            sql = "SELECT F_EMAIL FROM TC_SYS_USER WHERE F_ID = {}".format(UserId)
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()
            conn.close()
            return result[0][0]
        except Exception as e:
            logger.error("获取用户邮箱错误: %s" % e)
            return ""
        
    # 根据订单名获取用户id
    def getUserIdByOrdername(self,ordername):
        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            sql = "SELECT F_USERID FROM TF_ORDER WHERE F_ORDERNAME = '{}'".format(ordername)
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()
            conn.close()
            return result[0][0]
        except Exception as e:
            logger.error("获取用户ID错误: %s" % e)
            return 0