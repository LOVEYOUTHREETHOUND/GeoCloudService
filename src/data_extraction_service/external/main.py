import schedule
import time 

from src.data_extraction_service.external.schedule import orderProcess
from src.utils.logger import logger
from src.utils.db.oracle import create_pool
import src.config.config as config
import src.utils.GeoDBHandler 
import src.utils.GeoProcessor
from src.geocloudservice.recommend import recommendData

import geopandas as gpd
from shapely.geometry import Polygon


def main():
    tablename = ["TB_META_ZY02C","TB_META_GF1"]
    wkt = "POLYGON((108.0176 32.0361,108.0177 32.0361,108.0176 32.0362,108.0175 32.0361,108.0176 32.0361))"
    pool = create_pool()
    recommendData(tablename,wkt,None,pool)
        