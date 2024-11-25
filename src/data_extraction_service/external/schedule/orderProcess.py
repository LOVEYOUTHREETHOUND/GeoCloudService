import src.utils.db.mapper as mapper
from src.utils.db.oracle import create_pool
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
import threading


class OrderProcess:
    def __init__(self, pool):
        self.mapper = mapper.Mapper(pool)
        self.lock = threading.Lock()
        max_workers = config.JSON_MAX_WORKERS
        self.executor = ThreadPoolExecutor(max_workers)
        self.processed_orders = set()

    # 将未处理的订单名与订单数据名写入文件
    def writePendingOrderToRequire(self):
        try:
            logger.info("正在将未处理的订单名与订单数据名写入文件")
            idlist = self.mapper.getIdByStatus()
            datapath = config.JSON_WRITE_ORDERDATA_PATH
            orderpath = config.JSON_WRITE_ORDER_PATH
            
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
            
            def writeOrderJson(id):
                try:
                    orderresult = self.mapper.getAllByOrderIdFromOrder(id[0])[0]
                    # print (orderresult)
                    orderresult = convert_datetime_to_str(orderresult)
                    filepath = orderpath + '/' +'{}.json'.format(id[1])
                    if not os.path.exists(filepath):
                        with open(filepath, 'w') as f:
                            f.write(json.dumps(orderresult, indent=4, ensure_ascii=False))
                            logger.info('{}.json文件写入成功'.format(id[1]))
                except Exception as e:
                    logger.error('{}.json文件写入失败:{}'.format(id[1], e))
                    
            def writeOrderDataJson(id):
                try:
                    dataname = self.mapper.getDatanameByOrderId(id[0])
                    for data in dataname:
                        dataresult = self.mapper.getAllByOrderIdFromOrderData(id[0], data[0])[0]
                        dataresult = convert_datetime_to_str(dataresult)
                        filename = ''
                        if id[2] == '在线下载':
                            filename =  '{}__{}__{}.json'.format(id[1],data[0],'W')
                            jsonpath = datapath + "/" + filename
                        elif id[2] == '线下拷贝':
                            filename = '{}__{}__{}.json'.format(id[1],data[0],'N')
                            jsonpath = datapath + '/' + filename
                        if not os.path.exists(jsonpath):
                            with open(jsonpath, 'w') as f:
                                f.write(json.dumps(dataresult, indent=4, ensure_ascii=False))
                                logger.info('{}文件写入成功'.format(filename))
                except Exception as e:
                    logger.error('{}文件写入失败:{}'.format(filename, e))
            
            self.executor.map(writeOrderJson, idlist)
            self.executor.map(writeOrderDataJson, idlist) 
            # logger.info("文件写入成功")
        except Exception as e:
            logger.error("文件写入失败: %s" % e)

    # 创建Serv-U用户
    def createServUUser(self,ordername):
        try:
            overdueTime = config.SERVU_USER_OVERDUE_TIME
            logger.info("正在创建Serv-U用户 %s" % ordername)
            # 当前时间为起始时间
            starttime = int(datetime.now().timestamp())
            # 默认过期时间为两周
            endtime = datetime.now() + timedelta(days=overdueTime)
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
    def updateOrderStatusFromRespond(self):
        try:
            logger.info("正在更新订单状态")
            path = config.JSON_READ_PATH
            filelist = os.listdir(path)

            def allOrderdataIsReady(id):
                count = self.mapper.getCountByOrderId(id)
                return count == 0
            
            def process_file(filename):
                try:
                    strlist = filename.split('__')
                    ordername = strlist[0]
                    # 数据名常以.tar结尾，所以需要去掉后缀
                    if strlist[1].endswith(('tar')):
                        strlist[1] = strlist[1][:-4]
                    orderdata = strlist[1]
                    id = self.mapper.getIdByOrdername(ordername)
                    self.mapper.updateDataStatusByNameAndId(orderdata, id)
                    os.remove(path + '/' + filename)
                    if(allOrderdataIsReady(id)):
                        with self.lock:
                            if ordername not in self.processed_orders:
                                self.processed_orders.add(ordername)
                                logger.info("订单%s状态更新中" % ordername)
                                self.mapper.updateOrderStatusByOrdername(ordername)
                                self.createServUUser(ordername)
                                # self.sendEmail(ordername)  
                                logger.info("订单%s状态更新完成" % ordername)
                except Exception as e:
                    logger.error("订单%s状态更新出错: %s" % (ordername, e))

            self.executor.map(process_file, filelist)
            
            # logger.info("订单状态更新成功")
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
            
    # 清理过期测试订单
    def updateTestOrder(self):
        try:
            overdue_time = config.TEST_ORDER_OVERDUE_TIME
            overdue = (datetime.now() - timedelta(days=overdue_time)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            logger.info("正在处理过期测试订单,时间范围为%s之前" % overdue)
            orders = self.mapper.getTestOrder(overdue)
            
            def processTestOrder(order):
                logger.info("正在处理过期测试订单%s" % order['F_ID'])
                try:
                    with self.lock:
                        self.mapper.insertTestOrder(order)
                        f_id = order['F_ID']
                        count = self.mapper.getTestOrderCountByID(f_id)
                        if count > 0 :
                            count = 0
                            # logger.info("订单%s已成功插入TF_ORDER_TEST" % f_id)
                            self.mapper.deleteTestOrder(f_id)
                            logger.info("过期测试订单%s处理完成" % f_id)
                except Exception as e:
                    logger.error("过期测试订单%s处理失败: %s" % (order['F_ID'], e))
                    
            self.executor.map(processTestOrder, orders)
        except Exception as e:
            logger.error("过期测试订单处理错误: %s" % e)
                
    
    # 此函数用于生成readOrderData函数的测试数据 
    def justForTest(self):
        idlist = self.mapper.getIdByStatus()
        path = config.JSON_READ_PATH
        for id in idlist:
            result = self.mapper.getDatanameByOrderId(id[0])
            for data in result:
                file = open(path + '/' +'{}__{}.tar'.format(id[1],data[0]), 'w')
                file.close()
    