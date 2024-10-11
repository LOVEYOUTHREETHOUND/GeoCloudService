import src.utils.db.mapper as mapper
from  src.utils.logger import logger
from src.utils.Email import send_email
import os
import json
import src.config.config as config
from concurrent.futures import ThreadPoolExecutor
import string
import random
from datetime import datetime,timedelta
import hashlib


class OrderProcess:
    def __init__(self):
        self.mapper = mapper.Mapper()
        # max_workers = self.config.get("max_workers")
        max_workers = config.JSON_MAX_WORKERS
        self.executor = ThreadPoolExecutor(max_workers)

    # 将未处理的订单名与订单数据名写入文件
    def writeOrderData(self):
        try:
            logger.info("正在将未处理的订单名与订单数据名写入文件")
            # start = time.time()
            idlist = self.mapper.getIdByStatus()
            # datapath = self.config.get("writeorderdatapath")
            datapath = config.JSON_WRITE_ORDERDATA_PATH
            orderpath = config.JSON_WRITE_ORDER_PATH
            # orderpath = self.config.get("writeorderpath")
            
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
                filepath = orderpath + '/' +'{}.json'.format(id[1])
                if not os.path.exists(filepath):
                    with open(filepath, 'w') as f:
                        f.write(json.dumps(orderresult, indent=4, ensure_ascii=False))
                    
                dataname = self.mapper.getDatanameByOrderId(id[0])
                for data in dataname:
                    dataresult = self.mapper.getAllByOrderIdFromOrderData(id[0], data[0])[0]
                    dataresult = convert_datetime_to_str(dataresult)
                    jsonpath = datapath + '/' +'{}__{}.json'.format(id[1],data[0])
                    if not os.path.exists(jsonpath):
                        with open(jsonpath, 'w') as f:
                            f.write(json.dumps(dataresult, indent=4, ensure_ascii=False))
            
            self.executor.map(writeJsonFile, idlist) 
            # end = time.time()
            # print('writeOrderData cost time:', end - start)
            logger.info("文件写入成功")
        except Exception as e:
            logger.error("文件写入失败: %s" % e)

    # 创建Serv-U用户
    def createServUUser(self,ordername):
        try:
            logger.info("正在创建Serv-U用户 %s" % ordername)
            # 当前时间为起始时间
            starttime = int(datetime.now().timestamp())
            # 默认过期时间为两周
            endtime = datetime.now() + timedelta(days=14)
            endtime = int(endtime.timestamp())
            
            # 返回值为原始密码和md5加密后的密码
            # 加密规则为：nn+md5(nn+password)，其中nn为随机的两个字母
            def createPwd():
                characters = string.ascii_letters + string.digits  # 包含大小写字母和数字
                password = ''.join(random.choice(characters) for _ in range(8))
                head = ''.join(random.choice(string.ascii_letters) for _ in range(2))
                md5 = head + password
                md5_obj = hashlib.md5()
                md5_obj.update(md5.encode('utf-8'))
                md5 = head + md5_obj.hexdigest()
                return password,md5

            pwd, md5 = createPwd()
            self.mapper.insertServUInfo(starttime, endtime, ordername, md5)
            self.mapper.insertServUPwd(ordername, pwd, md5)
            logger.info("Serv-U用户 %s 创建成功" % ordername)
        except Exception as e:
            logger.error("Serv-U用户{}创建失败:{}" .format(ordername, e))
                
    
    # 根据文件中的订单名和订单数据名更新订单状态
    def readOrderData(self):
        try:
            logger.info("正在更新订单状态")
            # start = time.time()
            # path = self.config.get("readpath")
            path = config.JSON_READ_PATH
            filelist = os.listdir(path)
            
            def process_file(filename):
                strlist = filename.split('__')
                ordername = strlist[0]
                # 数据名常以.tar.gz结尾，所以需要去掉后缀
                if strlist[1].endswith(('tar')):
                    strlist[1] = strlist[1][:-4]
                orderdata = strlist[1]
                id = self.mapper.getIdByOrdername(ordername)
                self.mapper.updateDataStatusByNameAndId(orderdata, id)
                os.remove(path + '/' + filename)
                if(self.mapper.getCountByOrderId(id) == 0):
                    self.mapper.updateOrderStatusByOrdername(ordername)
                    self.createServUUser(ordername)
                    # self.sendEmail(ordername)
                
            self.executor.map(process_file, filelist)
            
            # end = time.time()
            # print('readOrderData cost time:', end - start)
            logger.info("订单状态更新成功")
        except Exception as e:
            logger.error("订单状态更新失败: %s" % e)

    # 向数据准备完成的用户发送邮件
    def sendEmail(self,ordername):
        try:
            logger.info("订单%s正在发送邮件" % ordername)
            # 获取用户邮箱
            userId = self.mapper.getUserIdByOrdername(ordername)
            email = self.mapper.getEmailByUserId(userId)
            subject = "地质云卫星数据服务-订单数据准备完成-{}".format(ordername)
            massage = "您好，您的地质云卫星数据服务订单【{}】数据准备完成,请在14天内进行数据下载,14天后将不能下载。【中国地质调查局自然资源航空物探遥感中心】".format(ordername)
            send_email(subject, massage, email)
            logger.info("订单{}邮件发送成功".format(ordername))
        except Exception as e:
            logger.error("订单{}邮件发送失败:{}".format(ordername, e))
    
    # 此函数用于生成readOrderData函数的测试数据 
    def justForTest(self):
        idlist = self.mapper.getIdByStatus()
        # path = self.config.get("readpath")
        path = config.JSON_READ_PATH
        for id in idlist:
            result = self.mapper.getDatanameByOrderId(id[0])
            for data in result:
                file = open(path + '/' +'{}__{}.tar.gz'.format(id[1],data[0]), 'w')
                file.close()
    