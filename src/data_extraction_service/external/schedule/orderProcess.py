# import geocloudservice.db.mapper as mapper
# import geocloudservice.db.oracle as oracle
import utils.db.mapper as mapper
import utils.db.oracle as oracle
import os
import json
import config.Json_config as Json_config
from concurrent.futures import ThreadPoolExecutor
import time

class OrderProcess:
    def __init__(self):
        self.pool = oracle.create_pool()
        self.mapper = mapper.Mapper(self.pool)
        self.config = Json_config.JsonConfig
        max_workers = self.config.get("max_workers")
        self.executor = ThreadPoolExecutor(max_workers)

    # 将未处理的订单名与订单数据名写入文件
    def writeOrderData(self):
        # start = time.time()
        idlist = self.mapper.getIdByStatus()
        # config = Json_config.JsonConfig
        path = self.config.get("writepath")
        for id in idlist:
            result = self.mapper.getDatanameByOrderId(id[0])
            orderdata = {
                'order_name': id[1],
                'order_data': [item[0] for item in result]
            }
            with open(path + '/' +'{}.json'.format(id[1]), 'w') as f:
                f.write(json.dumps(orderdata, indent=4, ensure_ascii=False))
        # end = time.time()
        # print('writeOrderData cost time:', end - start)
                
    # 根据文件中的订单名和订单数据名更新订单状态
    def readOrderData(self):
        start = time.time()
        path = self.config.get("readpath")
        filelist = os.listdir(path)
        
        def process_file(filename):
            strlist = filename.split('__')
            ordername = strlist[0]
            orderdata = strlist[1]
            id = self.mapper.getIdByOrdername(ordername)
            self.mapper.updateDataStatusByNameAndId(orderdata, id)
            os.remove(path + '/' + filename)
            # print(self.mapper.getCountByOrderId(id))
            if(self.mapper.getCountByOrderId(id) == 0):
                self.mapper.updateOrderStatusByOrdername(ordername)
            
        self.executor.map(process_file, filelist)
        
        end = time.time()
        print('readOrderData cost time:', end - start)
    
    # 此函数用于生成readOrderData函数的测试数据 
    def justForTest(self):
        idlist = self.mapper.getIdByStatus()
        path = self.config.get("writepath")
        for id in idlist:
            result = self.mapper.getDatanameByOrderId(id[0])
            for data in result:
                file = open(path + '/' +'{}__{}'.format(id[1],data[0]), 'w')
                file.close()

        
        
        
        
        


        