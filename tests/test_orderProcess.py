# from src.geocloudservice.schedule.orderProcess import OrderProcess
from data_extraction_service.external.schedule.orderProcess import OrderProcess
import time

myOrderProcess = OrderProcess()
myOrderProcess.sendEmail("20240924WP00003")
# myOrderProcess.justForTest()
# myOrderProcess.readOrderData()

# t2 = 1727230889
# s_l = time.localtime(t2)
# ts = time.strftime("%Y-%m-%d %H:%M:%S", s_l)
# print(ts )