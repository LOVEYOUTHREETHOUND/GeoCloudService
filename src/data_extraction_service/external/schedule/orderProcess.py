import utils.db.mapper as mapper
import utils.db.oracle as oracle
import os
import json
import config.Json_config as Json_config
from concurrent.futures import ThreadPoolExecutor
import time
import string
import random
from datetime import datetime,timedelta
import hashlib

class OrderProcess:
    def __init__(self):
        self.pool = oracle.create_pool()
        self.mapper = mapper.Mapper(self.pool)
        self.config = Json_config.JsonConfig
        max_workers = self.config.get("max_workers")
        self.executor = ThreadPoolExecutor(max_workers)

    # 将未处理的订单名与订单数据名写入文件
    def writeOrderData(self):
        start = time.time()
        idlist = self.mapper.getIdByStatus()
        datapath = self.config.get("writeorderdatapath")
        orderpath = self.config.get("writeorderpath")
        
        def convert_datetime_to_str(data):
            if isinstance(data, dict):
                for key, value in data.items():
                    if isinstance(value, datetime):
                        data[key] = value.isoformat()
                    elif isinstance(value, (dict, list)):
                        convert_datetime_to_str(value)
            elif isinstance(data, list):
                for index, item in enumerate(data):
                    if isinstance(item, datetime):
                        data[index] = item.isoformat()
                    elif isinstance(item, (dict, list)):
                        convert_datetime_to_str(item)
            return data
        
        def writeJsonFile(id):
            orderresult = self.mapper.getAllByOrderIdFromOrder(id[0])[0]
            # print (orderresult)
            orderresult = convert_datetime_to_str(orderresult)
            with open(orderpath + '/' +'{}.json'.format(id[1]), 'w') as f:
                f.write(json.dumps(orderresult, indent=4, ensure_ascii=False))
                
            dataname = self.mapper.getDatanameByOrderId(id[0])
            for data in dataname:
                dataresult = self.mapper.getAllByOrderIdFromOrderData(id[0], data[0])[0]
                dataresult = convert_datetime_to_str(dataresult)
                with open(datapath + '/' +'{}__{}.json'.format(id[1],data[0]), 'w') as f:
                    f.write(json.dumps(dataresult, indent=4, ensure_ascii=False))
           
        self.executor.map(writeJsonFile, idlist) 
        end = time.time()
        print('writeOrderData cost time:', end - start)

    # 创建Serv-U用户
    def createServUUser(self,ordername):
        starttime = int(datetime.now().timestamp())
        endtime = datetime.now() + timedelta(days=14)
        endtime = int(endtime.timestamp())
        
        def createPwd():
            characters = string.ascii_letters + string.digits  # 包含大小写字母和数字
            password = ''.join(random.choice(characters) for _ in range(8))
            head = ''.join(random.choice(string.ascii_letters) for _ in range(2))
            pwd = head + password
            md5_obj = hashlib.md5()
            md5_obj.update(pwd.encode('utf-8'))
            pwd = head + md5_obj.hexdigest()
            return password,pwd

        pwd, md5 = createPwd()
        # print("生成的随机密码:", pwd)
        # print("生成的md5密码:", md5)
        self.mapper.insertServUInfo(starttime, endtime, ordername, md5)
        # print("Serv-U用户创建成功")
        self.mapper.insertServUPwd(ordername, pwd)
        # print("Serv-U密码创建成功")
                
    
    # 根据文件中的订单名和订单数据名更新订单状态
    def readOrderData(self):
        start = time.time()
        path = self.config.get("readpath")
        filelist = os.listdir(path)
        
        def process_file(filename):
            strlist = filename.split('__')
            ordername = strlist[0]
            # 数据名常以.tar.gz结尾，所以需要去掉后缀
            if strlist[1].endswith(('tar.gz')):
                strlist[1] = strlist[1][:-7]
            orderdata = strlist[1]
            id = self.mapper.getIdByOrdername(ordername)
            self.mapper.updateDataStatusByNameAndId(orderdata, id)
            os.remove(path + '/' + filename)
            if(self.mapper.getCountByOrderId(id) == 0):
                print("111")
                self.mapper.updateOrderStatusByOrdername(ordername)
                self.createServUUser(ordername)
            
        self.executor.map(process_file, filelist)
        
        end = time.time()
        print('readOrderData cost time:', end - start)
    
    # 此函数用于生成readOrderData函数的测试数据 
    def justForTest(self):
        idlist = self.mapper.getIdByStatus()
        path = self.config.get("readpath")
        for id in idlist:
            result = self.mapper.getDatanameByOrderId(id[0])
            for data in result:
                file = open(path + '/' +'{}__{}.tar.gz'.format(id[1],data[0]), 'w')
                file.close()

        
        
        
        
        


        