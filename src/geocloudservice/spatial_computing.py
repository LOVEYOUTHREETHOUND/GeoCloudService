# This file is designed to make spatial computations for data recommendation

import geopandas as gpd
import oracledb
import shapely
import typing

import src.config as config


def coordinates_to_polygon(ordinates: list):
    """
    Transform coordinate list that is query from ORACLE DB SDO Geometry
    To
    shapely.Polygon
    e.g.: [long_1, lat_1, long_2, lat_2 ... long_5, lat_5] -> [(long_1, lat_1), (long_2, lat_2)...]
    """
    length = len(ordinates)
    return shapely.Polygon([(ordinates[i], ordinates[i + 1]) for i in range(0, length - 1, 2)])


def _query_by_satellite(satellite_name: str) -> str:
    table_name = f"TB_META_{satellite_name}"
    query_sql = f'SELECT "F_DATANAME","F_RECEIVETIME","F_SPATIAL_INFO" FROM {table_name}'
    return query_sql


def _build_gdf_by_db_res(res: list) -> gpd.GeoDataFrame:
    polygons = [coordinates_to_polygon(r[-1].SDO_ORDINATES.aslist()) for r in res]
    df = gpd.GeoDataFrame(geometry=polygons, crs=config.crs)
    return df


def find_data_by_satellite(satellite_names: typing.List[str], pool: oracledb.ConnectionPool) -> gpd.GeoDataFrame:

    # TODO: find META TABLE by satellite name
    # e.g.: GF1 -> TB_META_GF1
    # Currently, use template TB_META_{satellite_name}

    # Use UNION to query multiple tables

    if len(satellite_names) == 1:
        query_sql = f'{_query_by_satellite(satellite_names[0])} ORDER BY "F_RECEIVETIME" FETCH FIRST :limit_num ROWS ONLY'
    else:
        table_name = ' UNION '.join([_query_by_satellite(satellite_name) for satellite_name in satellite_names])
        query_sql = f'{table_name} ORDER BY "F_RECEIVETIME" FETCH FIRST :limit_num ROWS ONLY'
    print(query_sql)
    with pool.acquire() as conn:
        with conn.cursor() as cur:
            cur.execute(query_sql, limit_num=100)
            res = cur.fetchall()
    polygons = [coordinates_to_polygon(r[-1].SDO_ORDINATES.aslist()) for r in res]  # FIXME: Index -1 could be in error
    df = gpd.GeoDataFrame(geometry=polygons, crs=config.crs)
    return df


def target_geometry(points: typing.List[typing.Tuple[float, float]]) -> gpd.GeoDataFrame:
    """
    Transform coordinate list to shapely.Polygon then to GeoDataFrame
    e.g.: [(long_1, lat_1), (long_2, lat_2)...] -> shapely.Polygon
    @param points: [(long_1, lat_1), (long_2, lat_2)...]
    #return:
    # GeoDataFrame
    """
    shp = shapely.Polygon(points)
    return gpd.GeoDataFrame(geometry=[shp], crs=config.crs)


def wkt_to_geometry(wkt: str) -> gpd.GeoDataFrame:
    """
    Transform WKT to GeoDataFrame
    @param wkt: WKT string
    #return: GeoDataFrame
    """
    return gpd.GeoDataFrame(geometry=gpd.GeoSeries.from_wkt(wkt), crs=config.crs)


def intersection_and_area(target_df: gpd.GeoDataFrame, data_df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Find intersection between target and data, and compute the intersection area
    @param target_df: target GeoDataFrame by user
    @param data_df: data GeoDataFrame of satellite
    #return: GeoDataFrame of intersection
    """

    try:
        intersection_df = data_df.intersection(target_df)
        intersection_df = intersection_df[intersection_df.geometry.is_empty() == False]
    except:
        pass

