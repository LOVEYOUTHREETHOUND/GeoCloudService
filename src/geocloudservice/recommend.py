from src.utils.GeoProcessor import GeoProcessor
from src.utils.GeoDBHandler import GeoDBHandler
from src.utils.logger import logger
from concurrent.futures import ThreadPoolExecutor
from src.utils.Email import send_email
from src.utils.db.oracle import executeNonQuery, executeQuery
from src.config.config import satelliteToNodeId

def fetchDataFromDB(pool, sql:str ,param=None):
    """从数据库中获取数据和字段名"""
    try:
        with pool.acquire() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, param)
                # 默认最后一个字段名是几何字段，舍弃
                columns = [desc[0] for desc in cur.description[:-1]]
                data = cur.fetchall()
                # logger.info(f'sql: {sql}, param: {param}, data: {data}')
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
    selectSql = generateSqlQuery(dataname, tablename)
    ordersql = ' ORDER BY "F_RECEIVETIME" DESC FETCH FIRST :limit_num ROWS ONLY'
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
        selectSql = generateSqlQuery(dataname, tablename, whereSql)
        orderSql = ' ORDER BY "F_RECEIVETIME" DESC '
        sql = f'{selectSql} {orderSql}'
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
    
def querySubscribedData(tablename: list, wkt: str, areacode: str, startTime: str, endTime: str, cloudPercent: str, pool) -> list:
    """订阅功能具体实现

    Args:
        tablename (list): 需要查询的数据表名
        wkt (str): 检索区域的wkt
        areacode (str): 检索区域的行政区划代码
        startTime (str): 影像数据开始时间
        endTime (str): 影像数据结束时间
        cloudPercent (str): 云量
        userid (str): 用户id
        pool (_type_): 数据库连接池
    """
    
    try:
        if wkt is None and areacode is None:
            logger.error('wkt和areacode不能同时为空')
            return None
        dataname = ["F_DATANAME", "F_SPATIAL_INFO"]
        whereSql = ' WHERE  F_RECEIVETIME BETWEEN TO_DATE(:startTime, \'YYYY-MM-DD HH24:MI:SS\') AND TO_DATE(:endTime, \'YYYY-MM-DD HH24:MI:SS\') AND F_CLOUDPERCENT <= :cloudPercent'
        selectSql = generateSqlQuery(dataname, tablename, whereSql)
        orderSql = ' ORDER BY "F_RECEIVETIME" DESC '
        sql = f'{selectSql} {orderSql}'
        print(sql)
        ImageInfo, columns = fetchDataFromDB(pool, sql, {'startTime': startTime, 'endTime': endTime, 'cloudPercent': cloudPercent})
        print(ImageInfo)
        geodbhandler = GeoDBHandler()
        ImageGdf = geodbhandler.dbDataToGeoDataFrame(ImageInfo, columns)
        target_area = getTargetArea(geodbhandler, wkt, areacode, pool)
        geoprocessor = GeoProcessor()
        intersected_data = geoprocessor.findIntersectedData(target_area, ImageGdf)
        dataDict = geoprocessor.GeoDataFrameToDict(intersected_data)
        nameList = [data['F_DATANAME'] for data in dataDict]
        return nameList
    except Exception as e:
        logger.error(f'订阅数据失败: {e}')
        return None

def sendEmailToUser(userid: str, data: list, pool):
    """向用户发送邮件

    Args:
        userid (str): 用户id
        data (list): 查询数据名
        pool (_type_): 数据库连接池
    """
    def getUserEmail(userid: str):
        """获取用户的邮箱地址"""
        sql = "SELECT F_EMAIL FROM TC_SYS_USER WHERE F_ID = :F_ID"
        emailAddr, _ = fetchDataFromDB(pool, sql, {'F_ID': userid})
        return emailAddr[0][0]
    subject = "地质云卫星数据服务-数据订阅"
    message = f"您好，您订阅的数据{data}已经上线【中国地质调查局自然资源航空物探遥感中心】"
    emailAddr = getUserEmail(userid)
    # send_email(subject, message, emailAddr)
    logger.info(f'邮件发送成功: {message} to {emailAddr}')

def updateSubOrderStatus(pool, subid: str):
    """更新订阅订单状态"""
    try:
        logger.info(f'正在更新订阅{subid}状态')
        sql = 'UPDATE SUBSCRIBE_ORDER SET STATUS = 1 WHERE SUBID = :subid'
        executeNonQuery(pool, sql, {"subid": subid})
    except Exception as e:
        logger.error(f'更新订阅订单状态失败: {e}')
        return None
    
    
def ProcessDueSubscriptions(pool):
    """处理过期的订阅"""
    try:
        dataname = ["USERID","SUBID","AREACODE","ISWKT","NODENAMES","CLOUDPERCENT",
                    "SUBTIME","SUBSTARTTIME","SUBENDTIME","WKT","STATUS"]
        tabelname = ["SUBSCRIBE_ORDER"]
        whereSql = ' WHERE  SUBENDTIME < SYSDATE AND STATUS = 0'
        selectSql = generateSqlQuery(dataname, tabelname, whereSql)
        sql = selectSql
        data = executeQuery(pool, sql)
        
        def processASubscription(sub):
            IsWKT = sub[3]
            if(IsWKT == 1):
                wkt = sub[9]
                areacode = None
            else :
                wkt = None
                areacode = sub[2]
            userid = sub[0]
            subid = sub[1]
            tablenames = sub[4].split(',')
            startTime = str(sub[7])
            endTime = str(sub[8])
            cloudPercent = sub[5]
            logger.info(f'startTime: {startTime}, endTime: {endTime}, cloudPercent: {cloudPercent}')
            datanames = querySubscribedData(tablenames, wkt, areacode, startTime, endTime, cloudPercent, userid, pool)
            sendEmailToUser(userid, datanames, pool)
            updateSubOrderStatus(pool, subid)
        executor = ThreadPoolExecutor()
        executor.map(processASubscription, data)
        
    except Exception as e:
        logger.error(f'处理过期订阅失败: {e}')
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
        res = executeQuery(pool, sql, {'areacode': areacode})[0][0]
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
            try:
                newDict['NODEID'] = satelliteToNodeId[newDict['F_SATELLITEID']][newDict['F_SENSORID']]
            except Exception as e:
                logger.error(f"获取NODEID失败, 不识别的卫星名或传感器名: {e}")

            newDict['RN'] = index + 1
            res.append(newDict)
        return res
    except Exception as e:
        logger.error(f'格式化字典列表失败: {e}')
        return None

def generateSqlQuery(dataname: list, tablename: list, wheresql: str = None) ->str:
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
            tableSqlList.append(f'select {columns} FROM {table} {wheresql}')
        sql = ' UNION ALL '.join(tableSqlList)
        return sql
    except Exception as e:
        logger.error(f'查询语句生成失败: {e}')
        return None