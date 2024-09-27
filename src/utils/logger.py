import logging
import datetime
logger = logging.getLogger("main")
stream_handler = logging.StreamHandler()

time_f_str = "%Y-%m-%d_%H-%M-%S"
file_handler = logging.FileHandler(f"{datetime.datetime.now().strftime(time_f_str)}.log")

stream_handler.setLevel(logging.INFO)
file_handler.setLevel(logging.DEBUG)

log_fmt = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")
stream_handler.setFormatter(log_fmt)
file_handler.setFormatter(log_fmt)

logger.addHandler(stream_handler)
logger.addHandler(file_handler)

logger.setLevel(logging.DEBUG)

