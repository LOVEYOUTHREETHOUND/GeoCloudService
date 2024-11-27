import schedule
import time 

from src.data_extraction_service.external.schedule import orderProcess
from src.utils.logger import logger
from src.utils.db.oracle import create_pool, executeQueryAsDict
import src.config.config as config
import src.utils.GeoDBHandler 
import src.utils.GeoProcessor
from src.geocloudservice.recommend import ProcessDueSubscriptions, recommendData

import geopandas as gpd
from shapely.geometry import Polygon
from src.utils.IdMaker import getPkId


def main():
    tablename = ["TB_META_ZY02C","TB_META_GF1"]
    wkt = "POLYGON ((93.2814 58.5466, 94.2746 58.4038, 93.9459 57.8909, 92.9661 58.0319, 93.2814 58.5466))"
    pool = create_pool()
    print(recommendData(tablename,wkt,None,pool))
    # ProcessDueSubscriptions(pool)
