import geocloudservice.db.mapper as mapper
import os
import json
import Json_config

# 将未处理的订单名与订单数据名写入文件
def writeOrderData():
    idlist = mapper.getIdByStatus()
    config = Json_config.JsonConfig
    path = config.get("writepath")
    for id in idlist:
        result = mapper.getDatanameByOrderId(id[0])
        orderdata = {
            'order_name': id[1],
            'order_data': [item[0] for item in result]
        }
        with open(path + '/' +'{}.json'.format(id[1]), 'w') as f:
            f.write(json.dumps(orderdata, indent=4, ensure_ascii=False))
            
# 根据文件中的订单名和订单数据名更新订单状态
def readOrderData():
    config = Json_config.JsonConfig
    path = config.get("readpath")
    jsonlist = os.listdir(path)
    for jsonfile in jsonlist:
        with open(path + '/' + jsonfile, 'r') as f:
            data = json.load(f)
            name = data['order_name']
            id = mapper.getIdByOrdername(name)
            orderdata = data['order_data']
            mapper.updateStatusByOrdername(name)
            for item in orderdata:
                mapper.updateStatusByNameAndId(item, id)
            os.remove(path + '/' + jsonfile)
            


        