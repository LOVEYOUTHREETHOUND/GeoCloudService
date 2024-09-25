import schedule

from src.geocloudservice.schedule import orderProcess

schedule.every(30).minutes.do(orderProcess.writeOrderData)
schedule.every(40).minutes.do(orderProcess.readOrderData)

