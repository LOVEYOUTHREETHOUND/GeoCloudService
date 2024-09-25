from geocloudservice.db import oracle

def getIdByStatus():
    pool = oracle.create_pool()
    conn = pool.connection()
    cursor = conn.cursor()
    # 从TF_ORDER里面查询最近20条未处理的订单ID和订单名
    sql = "SELECT F_ID, F_ORDERNAME FROM (SELECT F_ID, F_ORDERNAME FROM TF_ORDER WHERE F_STATUS = -1 ORDER BY F_ORDERNAME DESC) WHERE ROWNUM <= 20"
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result

    
def getDatanameByOrderId(f_orderid):
    pool = oracle.create_pool()
    conn = pool.connection()
    cursor = conn.cursor()
    # 根据订单ID查询订阅数据名
    sql = "SELECT F_DATANAME FROM TF_ORDERDATA WHERE F_ORDERID = {} AND F_STATUS = 1".format(f_orderid)
    print(sql)
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result