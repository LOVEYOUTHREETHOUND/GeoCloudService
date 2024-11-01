import src.config.config as config
import src.utils.logger as logger
import threading 

class Mapper:
    def __init__(self, pool):
        self.pool = pool
        self.lock = threading.Lock()   
        
    # 执行查询语句
    def executeQuery(self, sql, params=None):
        try:
            with self.pool.acquire() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    result = cursor.fetchall()
                    # logger.info("查询成功: {}, params: {}".format(sql, params))
                    return result
        except Exception as e:
            logger.error("SQL查询错误: {}, SQL: {}, params: {}".format(e, sql, params))
    
    # 执行非查询语句
    def executeNonQuery(self, sql, params=None):
        try:
            with self.pool.acquire() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    conn.commit()
        except Exception as e:
            logger.error("SQL执行错误: {}, SQL: {}, params: {}".format(e, sql, params))
        
    # 从TF_ORDER里面查询最近20条未处理的订单ID和订单名
    def getIdByStatus(self):
        try:
            count = config.JSON_PROCESS_COUNT
            sql = "SELECT F_ID, F_ORDERNAME, F_GET_METHOD FROM (SELECT F_ID, F_ORDERNAME, F_GET_METHOD FROM TF_ORDER WHERE F_STATUS = 1 \
                    AND F_ORDERNAME IS NOT NULL ORDER BY F_ORDERNAME DESC) WHERE ROWNUM <= : count"
            result = self.executeQuery(sql, {'count': count})
            return result
        except Exception as e:
            logger.error("获取订单ID错误: %s" % e)
            return []

    #根据订单ID从TF_ORDERDATA里面查询订阅数据名 
    def getDatanameByOrderId(self,f_orderid):
        try:
            sql = "SELECT F_DATANAME FROM TF_ORDERDATA WHERE F_ORDERID = :F_ORDERID AND F_STATUS = 1"
            result = self.executeQuery(sql, {'F_ORDERID': f_orderid})
            return result
        except Exception as e:
            logger.error("获取订阅数据名错误: %s" % e)
            return []

    # 根据订单名在TF_ORDER中更新订单状态
    def updateOrderStatusByOrdername(self,f_ordername):
        try:
            with self.lock:
                sql = "UPDATE TF_ORDER SET F_STATUS = 6 WHERE F_ORDERNAME = :F_ORDERNAME"
                self.executeNonQuery(sql, {'F_ORDERNAME': f_ordername})
        except Exception as e:
            logger.error("更新订单状态错误: %s" % e)

    # 根据订阅数据名和订单ID在TF_ORDERDATA中更新订阅数据状态
    def updateDataStatusByNameAndId(self,f_dataname, f_orderid):
        try:
            with self.lock:
                sql = "UPDATE TF_ORDERDATA SET F_STATUS = 0 WHERE F_DATANAME = :F_DATANAME AND F_ORDERID = :F_ORDERID"
                self.executeNonQuery(sql, {'F_DATANAME': f_dataname, 'F_ORDERID': f_orderid})
        except Exception as e:
            logger.error("更新订单数据状态错误: %s" % e)
        
    # 根据订单名获取订单ID(f_orderid)
    def getIdByOrdername(self,f_ordername):
        try:
            sql = "SELECT F_ID FROM TF_ORDER WHERE F_ORDERNAME = :F_ORDERNAME"
            result = self.executeQuery(sql, {'F_ORDERNAME': f_ordername})
            return result[0][0]
        except Exception as e:
            logger.error("获取订单ID错误: %s" % e)
            return 0

    # 根据订单ID获取未完成的订单数量
    def getCountByOrderId(self,f_orderid):
        try:
            sql = "SELECT COUNT(*) FROM TF_ORDERDATA WHERE F_ORDERID = :F_ORDERID AND F_STATUS = 1"
            result = self.executeQuery(sql, {'F_ORDERID': f_orderid})
            return result[0][0]
        except Exception as e:
            logger.error("获取订单数量错误: %s" % e)
            return 0

    # 根据订单ID从TF_ORDER中获取所有信息
    # 返回格式为列名：数据
    def getAllByOrderIdFromOrder(self,f_orderid):
        try:
            with self.pool.acquire() as conn:
                with conn.cursor() as cursor:
                    sql = "SELECT * FROM TF_ORDER WHERE F_ID = :F_OEDERID"
                    cursor.execute(sql, {'F_OEDERID': f_orderid})
                    columns = [col[0] for col in cursor.description]
                    result = cursor.fetchall()
            
            data = [dict(zip(columns, row)) for row in result]
            return data
        except Exception as e:
            logger.error("获取订单信息错误: %s" % e)
            return data
    
    # 根据订单ID和数据名从TF_ORDERDATA中获取所有信息
    # 返回格式为列名：数据
    def getAllByOrderIdFromOrderData(self,f_orderid,f_dataname):
        try:
            with self.pool.acquire() as conn:
                with conn.cursor() as cursor:
                    sql = "SELECT * FROM TF_ORDERDATA \
                        WHERE F_ORDERID = :F_ORDERID AND F_DATANAME = :F_DATANAME"
                    cursor.execute(sql, {'F_ORDERID': f_orderid, 'F_DATANAME': f_dataname})
                    columns = [col[0] for col in cursor.description]
                    result = cursor.fetchall()
            
            data = [dict(zip(columns, row)) for row in result]
            return data
        except Exception as e:
            logger.error("获取订单数据错误: %s" % e)
            return data

    # 向数据库中插入数据以创建Serv-U用户
    # 1.向FTP_SUUSERS表中插入数据以创建用户
    # 2.向FTP_USERDIRACCESS表中插入数据以配置用户权限
    def insertServUInfo(self, starttime ,endtime, ordername, pwd):
        RtDailyCount = "0,14,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
        try:
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
            self.executeNonQuery(sql1, {
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
            self.executeNonQuery(sql2, {
                'ordername': ordername
            })
        except Exception as e:
            logger.error("Serv-U用户创建错误: %s" % e)
        
    # 向TF_ORDER表中对应用户插入密码
    def insertServUPwd(self, ordername, pwd, md5):
        try:
            sql1 = "UPDATE TF_ORDER SET F_PASSWORD = :PWD WHERE F_ORDERNAME = :ORDERNAME"
            self.executeNonQuery(sql1, {'PWD': pwd, 'ORDERNAME': ordername})
            # 保证TF_ORDER表中的密码和FTP_SUUSERS表中的密码一致
            sql2 = "UPDATE FTP_SUUSERS SET \"Password\" = :MD5 WHERE \"LoginID\" = :ORDERNAME"
            self.executeNonQuery(sql2, {'MD5': md5, 'ORDERNAME': ordername})
        except Exception as e:
            logger.error("Serv-U密码插入错误: %s" % e)
     
    # 从TF_ORDER表中查询测试订单
    def getTestOrder(self, one_week_ago):
        try:
            logger.info("正在查询测试订单")
            with self.pool.acquire() as conn:
                with conn.cursor() as cursor:
                    sql = """
                    SELECT * 
                    FROM TF_ORDER 
                    WHERE (F_PRODUCT_NAME LIKE '%测试%' 
                        OR F_PRODUCT_NAME LIKE '%test%' 
                        OR F_PRODUCT_NAME LIKE '%Test%')
                        AND (
                            (F_UPDATETIME IS NOT NULL AND F_UPDATETIME < TO_TIMESTAMP(:one_week_ago, 'YYYY-MM-DD HH24:MI:SS.FF3'))
                            OR (F_UPDATETIME IS NULL AND F_CREATTIME < TO_TIMESTAMP(:one_week_ago, 'YYYY-MM-DD HH24:MI:SS.FF3'))
                        )
                    """
                    cursor.execute(sql, {'one_week_ago': one_week_ago})
                    columns = [col[0] for col in cursor.description]
                    result = [dict(zip(columns, row)) for row in cursor.fetchall()]
  
            logger.info("测试订单查询完成")
            return result
        except Exception as e:
            logger.error("查询测试订单错误: %s" % e)
            return []
        
    # 向TF_ORDER_TEST表中插入测试订单
    def insertTestOrder(self,order):
        try:
            # logger.info("正在插入测试订单")
            columns = ', '.join([f'"{col}"' for col in order.keys()])
            values = ', '.join([f':{col}' for col in order.keys()])
            sql = f"""
            MERGE INTO TF_ORDER_TEST t
            USING (SELECT 1 FROM dual) d
            ON (t.F_ID = :F_ID)
            WHEN NOT MATCHED THEN
            INSERT ({columns})
            VALUES ({values})
            """
            
            self.executeNonQuery(sql, order)
            # logger.info("测试订单插入完成")
        except Exception as e:
            logger.error("插入测试订单错误: %s" % e)
        
    # 查询TF_ORDER_TEST表中是否含有对应订单
    def getTestOrderCountByID(self, F_ID):
        try:
            # logger.info("正在查询测试订单%s" % F_ID) 
            sql = f"SELECT COUNT(*) FROM TF_ORDER_TEST WHERE F_ID = :F_ID"
            result = self.executeQuery(sql, {'F_ID': F_ID})[0][0]
            # logger.info("测试订单%s查询完成" % F_ID)
            return result
        except Exception as e:
            logger.error("查询测试订单错误: %s" % e)
            return 0
        
    # 从TF_ORDER中删除测试订单
    def deleteTestOrder(self, F_ID):
        try:
            # logger.info("正在从TF_ORDER中删除测试订单%s" % F_ID)
            sql = f"DELETE FROM TF_ORDER WHERE F_ID = :F_ID"
            self.executeNonQuery(sql, {'F_ID': F_ID})
            # logger.info("测试订单%s成功从TF_ORDER中删除" % F_ID)
        except Exception as e:
            logger.error("删除测试订单错误: %s" % e)
              

    # 将文件信息插入TF_ORDERDATA表中
    def insertOrderData(self,data):
        try:
            orderId = data.get("F_ID")
            # logger.info("正在插入订单数据{}".format(orderId))
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
            self.executeNonQuery(sql, data)
            logger.info("订单数据{}插入成功".format(orderId))
        except Exception as e:
            logger.error("订单数据{}插入错误:{}".format(orderId, e))
            
    # 将文件信息插入TF_ORDER表中
    def insertOrder(self,data):
        try:
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
            self.executeNonQuery(sql, data)
            ordername = data.get("F_ORDERNAME")
            logger.info("订单{}插入成功".format(ordername))
        except Exception as e:
            logger.error("订单{}插入错误: {}".format(ordername, e))
     
    # 根据用户Id获取用户邮箱 
    def getEmailByUserId(self,UserId):
        try:
            sql = "SELECT F_EMAIL FROM TC_SYS_USER WHERE F_ID = :F_ID"
            result = self.executeQuery(sql, {'F_ID': UserId})
            return result[0][0]
        except Exception as e:
            logger.error("获取用户邮箱错误: %s" % e)
            return ""
        
    # 根据订单名获取用户id
    def getUserIdByOrdername(self,ordername):
        try:
            sql = "SELECT F_USERID FROM TF_ORDER WHERE F_ORDERNAME = :F_ORDERNAME"
            result = self.executeQuery(sql, {'F_ORDERNAME': ordername})
            return result[0][0]
        except Exception as e:
            logger.error("获取用户ID错误: %s" % e)
            return 0