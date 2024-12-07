import schedule
import time
from src.utils.db.oracle import create_pool
from src.data_extraction_service.external.schedule.orderProcess import OrderProcess
from src.geocloudservice.recommend import ProcessDueSubscriptions
from src.config import config

MyPool = create_pool()
MyOrderProcess = OrderProcess(MyPool)

schedule.every(config.SCHE_WRITE_ORDER_TIME).minutes.do(MyOrderProcess.writePendingOrderToRequire)
schedule.every(config.SCHE_READ_ORDER_TIME).minutes.do(MyOrderProcess.updateOrderStatusFromRespond)
schedule.every(config.SCHE_UPDATE_TESTORDER_TIME).days.do(MyOrderProcess.updateTestOrder)
schedule.every(config.SCHE_PROCESS_SUB_ORDER_TIME).days.do(ProcessDueSubscriptions,MyPool)

while True:
    schedule.run_pending()
    time.sleep(1)