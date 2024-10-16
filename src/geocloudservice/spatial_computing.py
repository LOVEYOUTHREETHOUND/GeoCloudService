# This file is designed to make spatial computations for data recommendation

import geopandas as gpd
import oracledb

def intersection(): pass


def find_data_by_satellite(satellite_name: str, conn: oracledb.Connection):

    # TODO: find META TABLE by satellite name
    # e.g.: GF1 -> TB_META_GF1

    table_name = "TB_META_GF1"

    query_sql = f'SELECT "F_SPATIAL_INFO" FROM {table_name} ORDER BY "F_RECEIVETIME" FETCH FIRST :limit_num ROWS ONLY'
    with conn.cursor() as cur:
        cur.execute(query_sql, limit_num=100)
        res = cur.fetchall()
        
