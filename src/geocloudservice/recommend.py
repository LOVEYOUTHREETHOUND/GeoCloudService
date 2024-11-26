from src.utils.GeoProcessor import GeoProcessor
from src.utils.GeoDBHandler import GeoDBHandler
from src.utils.logger import logger

def fetchDataFromDB(pool, sql:str ,param=None):
    """从数据库中获取数据和字段名"""
    try:
        with pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, param)
                # 默认最后一个字段名是几何字段，舍弃
                columns = [desc[0] for desc in cur.description[:-1]]
                data = cur.fetchall()
        return data, columns
    except Exception as e:
        logger.error(f'从数据库中获取数据失败: {e}, sql: {sql}, param: {param}')
        return None, None

def getTargetArea(geodbhandler: GeoDBHandler, wkt: str, areaCode: str, pool):
    """根据传入的wkt和行政区划代码获取目标区域的几何形状""" 
    try:  
        if wkt is not None:
            target_area = geodbhandler.wktToShapely(wkt)
        elif areaCode is not None:
            target_area = getShapelyAreaByCode(areaCode, pool)
        return target_area
    except Exception as e:
        logger.error(f'获取目标区域失败: {e}')
        return None

# def recommendData(tablename: list, wkt: str, areacode: str , pool) ->list:
def recommendData(tablename: list, wkt: str, areacode: str , pool, page: int, pagesize: int = 30) ->list:
    """一键推荐功能具体实现

    Args:
        tablename (list): 需要查询的表名列表(与卫星绑定)
        wkt (str): 检索区域的wkt
        areacode (str): 检索区域的行政区划代码
        pool (_type_): 数据库连接池
        areacode和wkt能且只能有一个不为空

    Returns:
        字典列表, 每一条字典代表一条数据
        覆盖率
    """
    
    if wkt is None and areacode is None:
        logger.error('wkt和areacode不能同时为空')
        return None
    dataname = ["F_DATANAME", "F_DID", "F_SCENEROW", "F_LOCATION", "F_PRODUCTID", "F_PRODUCTLEVEL",
                "F_CLOUDPERCENT", "F_TABLENAME", "F_DATATYPENAME", "F_ORBITID", "F_PRODUCETIME",
                "F_SENSORID", "F_DATASIZE", "F_RECEIVETIME", "F_DATAID", "F_SATELLITEID", "F_SCENEPATH","F_SPATIAL_INFO"]
    ordersql = ' ORDER BY "F_RECEIVETIME" FETCH FIRST :limit_num ROWS ONLY'
    selectSql = generateSqlQuery(dataname, tablename, whereSql="")
    sql = f'{selectSql} {ordersql}'
    geodbhandler = GeoDBHandler()
    geoprocessor = GeoProcessor()
    coverage_ratio = 0
    n = 1
    try:
        while coverage_ratio < 0.9 and n < 9:
            # print(f'第{n}次查询')
            limit_num = 10000 * n
            data, columns = fetchDataFromDB(pool, sql, {'limit_num': limit_num})
            data_gdf = geodbhandler.dbDataToGeoDataFrame(data, columns)
            target_area = getTargetArea(geodbhandler, wkt, areacode, pool)
            intersected_data = geoprocessor.findIntersectedData(target_area, data_gdf)
            coverage_ratio = geoprocessor.calCoverageRatio(target_area, intersected_data)
            n += 1
        result = geoprocessor.GeoDataFrameToDict(intersected_data)
        formatted_result = formatDictForView(result)
        start_index = (page - 1) * pagesize
        end_index = start_index + pagesize
        selected_result = formatted_result[start_index:end_index]
        return selected_result, coverage_ratio
    except Exception as e:
        logger.error(f'推荐数据失败: {e}')
        return None

def recommendCoverData(tablename: list, wkt: str, areacode: str , pool) ->dict:
    """一键推荐功能具体实现

    Args:
        tablename (list): 需要查询的表名列表(与卫星绑定)
        wkt (str): 检索区域的wkt
        areacode (str): 检索区域的行政区划代码
        pool (_type_): 数据库连接池
        areacode和wkt能且只能有一个不为空

    Returns:
        字典列表, 每一条字典代表一条数据
        覆盖率
    """
    
    if wkt is None and areacode is None:
        logger.error('wkt和areacode不能同时为空')
        return None
    dataname = ["F_DATANAME", "F_DID", "F_SCENEROW", "F_LOCATION", "F_PRODUCTID", "F_PRODUCTLEVEL",
                "F_CLOUDPERCENT", "F_TABLENAME", "F_DATATYPENAME", "F_ORBITID", "F_PRODUCETIME",
                "F_SENSORID", "F_DATASIZE", "F_RECEIVETIME", "F_DATAID", "F_SATELLITEID", "F_SCENEPATH","F_SPATIAL_INFO"]
    ordersql = ' ORDER BY "F_RECEIVETIME" FETCH FIRST :limit_num ROWS ONLY'
    selectSql = generateSqlQuery(dataname, tablename, whereSql="")
    sql = f'{selectSql} {ordersql}'
    geodbhandler = GeoDBHandler()
    geoprocessor = GeoProcessor()
    coverage_ratio = 0
    n = 1
    try:
        while coverage_ratio < 0.9 and n < 9:
            limit_num = 10000 * n
            data, columns = fetchDataFromDB(pool, sql, {'limit_num': limit_num})
            data_gdf = geodbhandler.dbDataToGeoDataFrame(data, columns)
            #combine_wkt, total_area = geoprocessor.calculateMergedArea(data_gdf)
            target_area = getTargetArea(geodbhandler, wkt, areacode, pool)
            intersected_data = geoprocessor.findIntersectedData(target_area, data_gdf)
            coverage_ratio = geoprocessor.calCoverageRatio(target_area, intersected_data)
            #print(combine_wkt)
            
            print(coverage_ratio)
            n +=1
        sizenum = len(intersected_data)
        
        combine_wkt, total_area = geoprocessor.calculateMergedArea(intersected_data)
        print(combine_wkt)
        return {
            "SIZENUM": sizenum,
            "WKTRESPONSE": combine_wkt,
            "TOTAL": total_area,
            "RN": 1
        }
    except Exception as e:
        logger.error(f'推荐数据合并失败: {e}')
        return None


def searchData(tablename: list, wkt :str, areacode : str, startTime: str, endTime: str, cloudPercent: str, pool) ->list:
    """检索功能具体实现

    Args:
        tablename (list): 需要查询的数据表名
        wkt (str): 检索区域的wkt
        areacode (str): 检索区域的行政区划代码
        startTime (str): 影像数据开始时间
        endTime (str): 影像数据结束时间
        cloudPercent (str): 云量

    Returns:
        list: 字典列表, 每一条字典代表一条数据
    """
    try:
        if wkt is None and areacode is None:
            logger.error('wkt和areacode不能同时为空')
            return None
        dataname = ["F_DATANAME", "F_DID", "F_SCENEROW", "F_LOCATION", "F_PRODUCTID", "F_PRODUCTLEVEL",
                    "F_CLOUDPERCENT", "F_TABLENAME", "F_DATATYPENAME", "F_ORBITID", "F_PRODUCETIME",
                    "F_SENSORID", "F_DATASIZE", "F_RECEIVETIME", "F_DATAID", "F_SATELLITEID", "F_SCENEPATH","F_SPATIAL_INFO"]
        whereSql = ' WHERE  F_RECEIVETIME BETWEEN TO_DATE(:startTime, \'YYYY-MM-DD HH24:MI:SS\') AND TO_DATE(:endTime, \'YYYY-MM-DD HH24:MI:SS\') AND F_CLOUDPERCENT <= :cloudPercent'
        orderSql = ' ORDER BY "F_RECEIVETIME" DESC '
        selectSql = generateSqlQuery(dataname, tablename,whereSql)
        sql = f'{selectSql}{orderSql}'
        ImageInfo, columns = fetchDataFromDB(pool, sql, {'startTime': startTime, 'endTime': endTime, 'cloudPercent': cloudPercent})
        geodbhandler = GeoDBHandler()
        ImageGdf = geodbhandler.dbDataToGeoDataFrame(ImageInfo, columns)
        target_area = getTargetArea(geodbhandler, wkt, areacode, pool)
        geoprocessor = GeoProcessor()
        intersected_data = geoprocessor.findIntersectedData(target_area, ImageGdf)
        result = geoprocessor.GeoDataFrameToDict(intersected_data)
        formatted_result = formatDictForView(result)
        return formatted_result
    except Exception as e:
        logger.error(f'检索数据失败: {e}')
        return None
    
       
def getShapelyAreaByCode(areacode: str, pool): 
    """根据传入的行政区划代码获取行政区划的几何形状

    Args:
        areacode (str): 行政区划的代码
        pool (_type_): 数据库连接池

    Returns:
        shapely.geometry对象: 行政区划的几何形状
    """
    try:    
        sql = 'select "GEOM" from TC_DISTRICT where F_DISTCODE = :areacode'
        with pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {'areacode': areacode})
                res = cur.fetchall()[0][0]
        geodbhandler = GeoDBHandler()
        return geodbhandler.sdoGeometryToShapely(res)
    except Exception as e:
        logger.error(f'无法从areacode获取对应几何形状: {e}')
        return None 

def formatDictForView(dictList: list):
    """将字典列表中的键值对进行处理, 使其适合前端展示

    Args:
        dictList (list): 字典列表

    Returns:
        list: 处理后的字典列表
    """
    try:
        res = []
        for index, data in enumerate(dictList):
            newDict = data.copy()
            
            if 'geometry' in newDict.keys():
                newDict['WKTRESPONSE'] = newDict.pop('geometry')

            # newDict['NODEID'] = ''
            newDict['RN'] = index + 1
            res.append(newDict)
        return res
    except Exception as e:
        logger.error(f'格式化字典列表失败: {e}')
        return None

def generateSqlQuery(dataname: list, tablename: list, whereSql:str) ->str:
    """根据传入的表名和字段名生成查询语句

    Args:
        dataname (list): 传入的字段名列表
        tablename (list): 传入的表名列表

    Returns:
        str: 查询语句
    """
    try:
        tableSqlList = []
        for table in tablename:
            table = table.upper()
            columns = ','.join([f'"{name}"' for name in dataname])
            tableSqlList.append(f'select {columns} FROM {table}{whereSql}')
        sql = ' UNION ALL '.join(tableSqlList)
        return sql
    except Exception as e:
        logger.error(f'查询语句生成失败: {e}')
        return None