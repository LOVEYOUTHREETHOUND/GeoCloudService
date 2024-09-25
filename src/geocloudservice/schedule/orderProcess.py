import geocloudservice.db.mapper as mapper
import json

# 将未处理的订单名与订单数据名写入文件
def writeOrderData():
    idlist = mapper.getIdByStatus()
    for id in idlist:
        result = mapper.getDatanameByOrderId(id[0])
        orderdata = {
            'order_name': id[1],
            'order_data': [item[0] for item in result]
        }
        with open('{}.json'.format(id[1]), 'w') as f:
            f.write(json.dumps(orderdata, indent=4, ensure_ascii=False))
            
writeOrderData()