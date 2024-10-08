import pathlib
original_data_base_path = [pathlib.Path(r"O:\原始影像数据库"), pathlib.Path(r"R:\GF_BDSS\原始影像数据库")]
order_base_path = pathlib.Path(r"Y:/shareJGF/order/extract/requirements")
order_base_order_path = pathlib.Path(r"Y:/shareJGF/order/extract/requirements/order")
order_base_order_data_path = pathlib.Path(r"Y:/shareJGF/order/extract/requirements/order_data")
order_base_response_path = pathlib.Path(r"Y:\shareJGF\order\extract\responses")
order_data_base_path = pathlib.Path(r"Y:/shareJGF/order/data")

interval = 10
order_worker_num = 10
file_copy_worker_num = 10
data_sync_filename_split_speratator = "__"

# Data type index definition
#e.g.: "GF1_PMS1_E53.8_N30.1_20240101_L1A13226391001.tar.gz"

class Index:
    SATELLITE = 0
    SENSOR = 1
    LONGITUDE = 2
    LATITUDE = 3
    DATETIME = 4
    NUMBER = 5

index = Index()

original_data_base_path_dic = {
    "GF1": {
        "PMS1": [r"R:\GF_BDSS\原始影像数据库\GF1原始影像数据\LEVEL1A\全色多光谱数据合并"],
        "PMS2": [r"R:\GF_BDSS\原始影像数据库\GF1原始影像数据\LEVEL1A\全色多光谱数据合并"]
    },
    "GF2": {
        "PMS1": [r"R:\GF_BDSS\原始影像数据库\GF2原始影像数据\全色多光谱数据合并"],
        "PMS2": [r"R:\GF_BDSS\原始影像数据库\GF2原始影像数据\全色多光谱数据合并"],
    },
    "GF1B": {
        "PMS": [r"R:\GF_BDSS\原始影像数据库\GF1B原始影像"],
    },
    "GF1C": {
        "PMS": [r"R:\GF_BDSS\原始影像数据库\GF1C原始影像"],
    },
    "GF1D": {
        "PMS": [r"R:\GF_BDSS\原始影像数据库\GF1D原始影像"],
    },
    "GF6": {
        "PMS": [r"R:\GF_BDSS\原始影像数据库\GF6原始影像数据", r"O:\原始影像数据库\GF6"],
        "MFV": [r"R:\GF_BDSS\原始影像数据库\GF6_WFV原始影像数据", r"O:\原始影像数据库\GF6\WFV"]
    },
    "ZY301a": {
        "MUX": [r"R:\GF_BDSS\原始影像数据库\ZY301a影像数据\ZY301a_MUX"],
        "NAD": [r"R:\GF_BDSS\原始影像数据库\ZY301a影像数据\ZY301a_MUX"]
    },
    "ZY302a": {
        "MUX": [r"R:\GF_BDSS\原始影像数据库\ZY301a影像数据\ZY302a_MUX"],
        "NAD": [r"R:\GF_BDSS\原始影像数据库\ZY301a影像数据\ZY302a_MUX"]
    },
    "ZY303a": {
        "MUX": [r"R:\GF_BDSS\原始影像数据库\ZY301a影像数据\ZY303a_MUX"],
        "NAD": [r"R:\GF_BDSS\原始影像数据库\ZY301a影像数据\ZY303a_MUX"]
    },
    # "GF3": [r"R:\GF_BDSS\原始影像数据库\其他\GF3"], # No datetime
    # "GF3B": [r"R:\GF_BDSS\原始影像数据库\其他\GF3B"], # Datetime format: 20220516
    # "GF3C": [r"R:\GF_BDSS\原始影像数据库\其他\GF3C"], # Datetime format: 20220516
    "CB04A": {
        "VNIC": [r"O:\原始影像数据库\CB04A\VNIC"],
        "WPM": [r"O:\原始影像数据库\CB04A\VNIC"]
    },
    "CSES_01": {
        "LAP": [r"O:\原始影像数据库\CSES_01\LAP"],
        "SCM": [r"O:\原始影像数据库\CSES_01\SCM"]
    },
    "GF5": {
        "AHSI": [r"O:\原始影像数据库\GF5\AHSI"],
        "VIMS": [r"O:\原始影像数据库\GF5\VIMS"]
    },
    "GF5B": {
        "AHSI": [r"O:\原始影像数据库\GF5B"],
    },
    "GF7": {
        "DLC": [r"O:\原始影像数据库\GF7\DLC"],
    },
    "GF701": {
        "BWD": [r"O:\原始影像数据库\GF7\BWD"],
        "MUX": [r"O:\原始影像数据库\GF7\MUX"],
    },
    "ZY1E": {
        "AHSI": [r"O:\原始影像数据库\ZY1E\AHSI"],
        "VNIC": [r"O:\原始影像数据库\ZY1E\VNIC"],
    },
    "ZY1F": {
        "AHSI": [r"O:\原始影像数据库\ZY1F\AHSI"],
        "VNIC": [r"O:\原始影像数据库\ZY1F\VNIC"],
    }
}

# Transfer all paths to pathlib

for sat, values in original_data_base_path_dic.items():
    for sensor, paths in values.items():
        original_data_base_path_dic[sat][sensor] = list(map(pathlib.Path, paths))

        
def _extract_datetime(datetime_str):
    year, month, day = datetime_str[:4], datetime_str[4:6], datetime_str[6:8]
    year_month = "_".join([year, month])
    year_month_day = "-".join([year, month, day])
    return year_month, year_month_day


def GF701_parser(data_name):
    # e.g.: GF701_026593_E091.5_N39.3_20240624124414_BWD_01_SC0_0001_2406254888
    satellite_name, code, longitude, latitude, datetime, sensor, *other, number = data_name.split("_")
    basic_path = original_data_base_path_dic[satellite_name][sensor]
    year_month, year_month_day = _extract_datetime(datetime)
    return satellite_name, sensor, longitude, latitude, year_month, year_month_day, number

name_parser = {
    "GF701": GF701_parser
}

file_suffix = "tar.gz"


