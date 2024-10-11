import schedule
import time 

from src.data_extraction_service.external.schedule import orderProcess
import src.config.config as config


def main():
    MyProcess = orderProcess.OrderProcess()
    writetime = config.SCHE_WRITE_ORDER_TIME
    readtime = config.SCHE_READ_ORDER_TIME
    schedule.every(writetime).minutes.do(MyProcess.writeOrderData())
    schedule.every(readtime).minutes.do(MyProcess.readOrderData())

    while True:
        schedule.run_pending()
        time.sleep(1)

