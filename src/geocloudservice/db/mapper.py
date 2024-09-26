from geocloudservice.db import oracle
import mapper_config 

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
        sql = "SELECT F_ID, F_ORDERNAME FROM (SELECT F_ID, F_ORDERNAME FROM TF_ORDER WHERE F_STATUS = -1 \
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
        sql = "SELECT F_DATANAME FROM TF_ORDERDATA WHERE F_ORDERID = {} AND F_STATUS = 0".format(f_orderid)
        # print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result

    # 根据订单名在TF_ORDER中更新订单状态
    def updateStatusByOrdername(self,f_ordername):
        # pool = oracle.create_pool()
        conn = self.pool.connection()
        cursor = conn.cursor()
        sql = "UPDATE TF_ORDER SET F_STATUS = 6 WHERE F_ORDERNAME = '{}'".format(f_ordername)
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()

    # 根据订阅数据名和订单ID在TF_ORDERDATA中更新订阅数据状态
    def updateStatusByNameAndId(self,f_dataname, f_orderid):
        # pool = oracle.create_pool()
        conn = self.pool.connection()
        cursor = conn.cursor()
        sql = "UPDATE TF_ORDERDATA SET F_STATUS = 1 WHERE F_DATANAME = '{}' AND F_ORDERID = '{}'".format(f_dataname, f_orderid)
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