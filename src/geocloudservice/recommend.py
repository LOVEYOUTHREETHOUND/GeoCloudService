from src.utils.GeoProcessor import GeoProcessor
from src.utils.GeoDBHandler import GeoDBHandler
from src.utils.logger import logger
from src.utils.db.oracle import create_pool

def recommend(tablename: list, wkt: None, areacode: None) :
    if wkt is None and areacode is None:
        logger.error('wkt和areacode不能同时为空')
        return None
    pool = create_pool()
    tableSqlList = []
    for table in tablename:
        table = table.upper()
        tableSqlList.append(f'select "F_DATANAME","F_DID","F_SCENEROW","F_LOCATION","F_PRODUCTID","F_PRODUCTLEVEL",\
            "F_CLOUDPERCENT","F_TABLENAME","F_DATATYPENAME","F_ORBITID","F_DATANAME","F_PRODUCETIME","F_SENSORID",\
                "F_DATASIZE","F_RECEIVETIME","F_DATAID","F_SATELLITEID","F_SCENEPATH" FROM {table} ')
    sql = ' UNION ALL '.join(tableSqlList)
    geodbhandler = GeoDBHandler()
    geoprocessor = GeoProcessor()
    sql = f'{sql} ORDER BY "F_RECEIVETIME" FETCH FIRST :limit_num ROWS ONLY'
    coverage_ratio = 0
    n = 1
    while coverage_ratio < 0.9 and n < 9:
        print(f'第{n}次查询')
        limit_num = 100 * n
        with pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {'limit_num': limit_num})
                columns = [desc[0] for desc in cur.description[:-1]]
                data = cur.fetchall()
        data_gdf = geodbhandler.dbDataToGeoDataFrame(data, columns)
        if wkt is not None:
            target_area = geodbhandler.wktToShapely(wkt)
        elif areacode is not None:
            target_area = getShapelyAreaByCode(areacode, pool)
        intersected_data = geoprocessor.findIntersectedData(target_area, data_gdf)
        coverage_ratio = geoprocessor.calCoverageRatio(target_area, intersected_data)
        n +=1
    return formatDictForView(geoprocessor.GeoDataFrameToDict(intersected_data))
        
def getShapelyAreaByCode(areacode: str, pool): 
    sql = 'select "GEOM" from TC_DISTRICT where F_DISTCODE = :areacode'
    with pool.acquire() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {'areacode': areacode})
            res = cur.fetchall()[0][0]
    geodbhandler = GeoDBHandler()
    return geodbhandler.sdoGeometryToShapely(res) 

def formatDictForView(dictList: list):
    res = []
    for index, data in enumerate(dictList):
        newDict = data.copy()
        
        if 'geometry' in newDict.keys():
            newDict['WKTRESPONSE'] = newDict.pop('geometry')

        newDict['NODEID'] = ''
        newDict['RN'] = index + 1
        res.append(newDict)
    return res