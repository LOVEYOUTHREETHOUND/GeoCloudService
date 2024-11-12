import schedule
import time 

from src.data_extraction_service.external.schedule import orderProcess
from src.utils.logger import logger
from src.utils.db.oracle import create_pool
import src.config.config as config
import src.utils.GeoDBHandler 
import src.utils.GeoProcessor

import geopandas as gpd
from shapely.geometry import Polygon


def main():
    sql = 'select "F_DATANAME","F_RECEIVETIME","F_SPATIAL_INFO" FROM TB_META_ZY02C'
    pool = create_pool()
    with pool.acquire() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description[:-1]]
            data = cur.fetchall()           
    geodbhandler = src.utils.GeoDBHandler.GeoDBHandler()
    data_gdf = geodbhandler.dbDataToGeoDataFrame(data, columns)
    # print(data_gdf.to_string())
    geoprocessor = src.utils.GeoProcessor.GeoProcessor()
    target_area = geodbhandler.wktToShapely('POLYGON((92.7451 57.6628, 93.7155 57.5231, 93.4004 57.0085, 92.4427 57.1464, 92.7451 57.6628))')
    intersected_data = geoprocessor.findIntersectedData(target_area, data_gdf)
    # print(intersected_data.to_string())
    coverage_ratio = geoprocessor.calCoverageRatio(target_area, intersected_data)
    intersected_list = geoprocessor.GeoDataFrameToList(intersected_data)
    print(intersected_list)