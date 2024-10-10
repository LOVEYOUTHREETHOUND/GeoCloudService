import schedule
import time 

from src.data_extraction_service.external.schedule import orderProcess
from src.config.schedule_config import schedule_config as config


def main():
    # schedule.every(30).minutes.do(orderProcess.writeOrderData)
    # schedule.every(40).minutes.do(orderProcess.readOrderData)
    MyProcess = orderProcess.OrderProcess()
    writetime = config["writeOrderDataTime"]
    readtime = config["readOrderDataTime"]
    schedule.every(writetime).minutes.do(MyProcess.writeOrderData())
    schedule.every(readtime).minutes.do(MyProcess.readOrderData())

    while True:
        schedule.run_pending()
        time.sleep(1)