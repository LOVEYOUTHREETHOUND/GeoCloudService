import config
import pathlib
import json
import logging
from src.utils.logger import logger
import shutil
from multiprocessing import Pool, Process, Queue, Lock
from typing import List
import schedule

# {
#     "order_name": "xxx",
#     "order_data": [
#           "GF1xxxx"
#     ]    
# }

def main():
    schedule.every(config.interval).minutes.do(data_extract)

def data_extract():
    # Read from Drivers
    order_path = config.order_base_path
    with Pool(config.worker_num) as p:
        p.map(extract_file, [(f, logger) for f in order_path.iterdir()])


def extract_file(f: str, logger: logging.Logger):
    with open(f, 'r') as order_file:
        data_needs_to_extract = json.load(order_file)
        
        # Iter all data
        order_name = data_needs_to_extract["order_name"]
        order_data = data_needs_to_extract["order_data"]
        logger.info(f"{order_name}: Start to process, total data number {len(order_data)}")
        copy_data(order_data, order_name)
        # Clear requirement file
    f.unlink()


# e.g.: "GF1_PMS1_E53.8_N30.1_20240101_L1A13226391001.tar.gz"
# Directory level:
# Sateillite
# |
# ----Sensor
#     |
#     ------Year_Month
#           |
#           ----------Year-Month-Day



def copy_data(data_names: str, order_name: str):
    total_copy_list = []
    total_number = len(data_names)
    for data_name in data_names:
        fields = data_name.split("_")
        data_name_with_suffix = f"{data_name}.{config.file_suffix}"
        satellite_name = fields[config.index.SATELLITE]
        if satellite_name in config.name_parser:
            satellite_name, sensor_name, longitude, latitude, year_month, year_month_day, number = config.name_parser[satellite_name](data_name)
        else:
            sensor_name = fields[config.index.SENSOR]
            longitude = fields[config.index.LONGITUDE]
            latitude = fields[config.index.LATITUDE]
            datetime = fields[config.index.DATETIME][:8]  # only keep yearmonthday   20240516
            year_month, year_month_day = config._extract_datetime(datetime)
            number = fields[config.index.NUMBER]
        try:
            paths = config.original_data_base_path_dic[satellite_name][sensor_name]
        except KeyError:
            logger.error(f"{order_name}: An unrecognized satellite {satellite_name}, data name is {data_name}.")
            continue
        #breakpoint()
        for path in paths:
            # Check whether year-month directory exists
            year_month_dir = path / year_month
            if year_month_dir.exists():
                year_month_day_dir = year_month_dir / year_month_day
                if year_month_day_dir.exists():
                    # Find target source file
                    target_path = year_month_day_dir / data_name_with_suffix
                    if target_path.exists():
                        # Copy file by new process
                        total_copy_list.append(target_path)
                    else:
                        logger.warning(f"{order_name} - {data_name}: Target data path {target_path} does not exist!")  # Target file is missing
                else:
                    logger.warning(f"{order_name} - {data_name}: No such year month day {year_month_day_dir} directory!") # No such year month day
            else:
                logger.warning(f"{order_name} - {data_name}: No such year month {year_month_dir} directory!") # No such year month
    if not total_copy_list:
        logger.info(f"{order_name}: No data matched for order")
    else:
        logger.info(f"{order_name}: Matched {len(total_copy_list)} items (total {total_number} items), start to copy...")
        copy_file(total_copy_list, order_name)
    logger.info(f"{order_name}: Complete!")


def copy_file(target_paths: list[pathlib.Path], order_name: str):
    copy_to_path = config.order_data_base_path / order_name
    if not copy_to_path.exists():
        copy_to_path.mkdir()
    with Pool(config.file_copy_worker_num) as p:
        p.map(copy_file_worker, [(f, copy_to_path, order_name, logger) for f in target_paths])

def copy_file_worker(f: pathlib.Path, copy_to_path: str, order_name: str, logger: logging.Logger):
    #breakpoint()
    logger.info(f"{order_name}: Copying data {f.name}...")
    try:
        shutil.copyfile(f, copy_to_path / f.name)
    except Exception as e:
        logger.error(f"{order_name}: Copy data {f.name} error! Exception: {e}")
    else:
        (config.order_base_response_path / f"{order_name}__{f.name}").touch()


if __name__ == "__main__":
    main()