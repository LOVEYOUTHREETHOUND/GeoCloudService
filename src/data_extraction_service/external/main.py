import schedule
import time 

from src.data_extraction_service.external.schedule import orderProcess
from src.utils.logger import logger
import src.config.config as config
from src.data_extraction_service.internal.main import data_extract


def main():
    MyProcess = orderProcess.OrderProcess()
    # writetime = config.SCHE_WRITE_ORDER_TIME
    # readtime = config.SCHE_READ_ORDER_TIME
    # logger.info(f"Start to run the schedule, write time: {writetime}, read time: {readtime}")
    # schedule.every(writetime).minutes.do(MyProcess.writeOrderData)
    # schedule.every(readtime).minutes.do(MyProcess.readOrderData)

    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
    # MyProcess.justForTest()
    # MyProcess.writeOrderData()
    MyProcess.updateTestOrder()
    # MyProcess.readOrderData()
    # data_extract()

