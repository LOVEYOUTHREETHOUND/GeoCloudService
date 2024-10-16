# import config
from src.data_extraction_service.internal import config
import pathlib
import json
import logging
from src.utils.logger import logger
import shutil
from multiprocessing import Pool, Process, Queue, Lock, Manager
import concurrent.futures
import os
from typing import List
import schedule
from src.utils.db.mapper import Mapper
from src.utils.db.oracle import create_pool
import time


# Order file name:
# <order_name>__<data_name>.json
# Order file content:
# Same as TF_ORDER table in Oracle DB

process_pool = None

def init_pool():
    global process_pool
    process_pool = create_pool()

def main():
    schedule.every(config.interval).minutes.do(data_extract)


def order_sync(db_client):
    "Synchoronize order from external service to internal service"
    order_path = config.order_base_order_path
    pass

def data_extract():
    "Iterate all files in order path, start new process for each order to extract data"
    # Read from Drivers
    order_path = config.order_base_path
    path = config.order_base_order_path
    starttime = time.time()
    with Pool(config.order_worker_num, initializer=init_pool) as p:
        extract_tasks = [(f) for f in order_path.iterdir()]
        sync_tasks = [(f) for f in path.iterdir() ]
        
        p.map(extract_file, extract_tasks)
        p.map(sync_order, sync_tasks)
        
    endtime = time.time()
    logger.info(f"Total time: {endtime - starttime}")
        
def extract_file(f: pathlib.Path):

    """
        Extract order data from order list
        Example file name: 
        "20240924WP00001__GF1_PMS1_E53.8_N30.1_20240101_L1A13226391001.json"
    """
    order_path = config.order_base_path
    
    if f.is_file() and f.suffix == ".json":
        logger.info(f"Start to extract file {f.name}")
        order_name, order_data = f.name.split(config.data_sync_filename_split_speratator)
        logger.info(f"{order_name}: Start to process data: {order_data}")
        copy_data(order_data, order_name)
        # Clear requirement file  
        path = order_path / f
        mapper = Mapper(process_pool)
        
        with open(path,"r", encoding="utf-8") as file:
            data = json.load(file)
            mapper.insertOrderData(data)
            
        f.unlink()
 
# 用于同步TF_ORDER表数据到内网 
def sync_order(f : pathlib.Path):
    order_path = config.order_base_order_path
    path = order_path / f
    mapper = Mapper(process_pool)
    with open(path,"r", encoding="utf-8") as file:
        data = json.load(file)
        mapper.insertOrder(data)
    os.remove(path)
        


# e.g.: "GF1_PMS1_E53.8_N30.1_20240101_L1A13226391001.tar.gz"
# Directory level:
# Sateillite
# |
# ----Sensor
#     |
#     ------Year_Month
#           |
#           ----------Year-Month-Day

def copy_data(data_name: str, order_name: str):
    "Find the target data file and copy to order data directory"
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
        return None
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
                    copy_file(target_path, order_name)
                else:
                    logger.warning(f"{order_name} - {data_name}: Target data path {target_path} does not exist!")  # Target file is missing
            else:
                logger.warning(f"{order_name} - {data_name}: No such year month day {year_month_day_dir} directory!") # No such year month day
        else:
            logger.warning(f"{order_name} - {data_name}: No such year month {year_month_dir} directory!") # No such year month


def copy_datas(data_names: str, order_name: str):
    "Find the target data file and copy to order data directory"
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


def copy_file(target_path: pathlib.Path, order_name: str):
    copy_to_path = config.order_data_base_path / order_name
    if not copy_to_path.exists():
        copy_to_path.mkdir()
    try:
        shutil.copyfile(target_path, copy_to_path / target_path.name)
    except Exception as e:
        logger.error(f"{order_name}: Copy data {target_path.name} error! Exception: {e}")
    else:
        (config.order_base_response_path / f"{order_name}__{target_path.name}").touch()


def copy_files(target_paths: list[pathlib.Path], order_name: str):
    "Start new process to copy each data file"
    copy_to_path = config.order_data_base_path / order_name
    if not copy_to_path.exists():
        copy_to_path.mkdir()
    with Pool(config.file_copy_worker_num) as p:
        p.map(copy_file_worker, [(f, copy_to_path, order_name, logger) for f in target_paths])


def copy_file_worker(f: pathlib.Path, copy_to_path: str, order_name: str, logger: logging.Logger):
    "Copy file to order directory and touch a file to indicate the data is copied"
    logger.info(f"{order_name}: Copying data {f.name}...")
    try:
        shutil.copyfile(f, copy_to_path / f.name)
    except Exception as e:
        logger.error(f"{order_name}: Copy data {f.name} error! Exception: {e}")
    else:
        (config.order_base_response_path / f"{order_name}__{f.name}").touch()


if __name__ == "__main__":
    main()