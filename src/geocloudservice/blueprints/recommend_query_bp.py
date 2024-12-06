from flask import Blueprint, request, Flask, g
from pydantic import BaseModel, Field, HttpUrl
from flask_cors import CORS
from flask_siwadoc import SiwaDoc
from typing import List, Optional, Tuple, Dict, Any, Union
from marshmallow import ValidationError

from src.utils.db.oracle import create_pool
from src.utils.CacheManager import CacheManager, SimpleCache
from src.geocloudservice.recommend import cacheFetchRecommendData, searchData, cacheFeachRecomCoverData


def rz_app():
    app = Flask(__name__,)
    CORS(app)
    siwa = SiwaDoc(app, title="FJY API", description="地质云航遥节点遥感数据服务系统接口文档")

    @app.post(f"/test")
    @siwa.doc(
        summary="测试接口",
        description="测试接口",
        tags=["test"],
    )
    def test():
        return {"code": 200, "msg": "successsss"}

    recommend_query_bp = recommend_query_blueprint(app, siwa)
    app.register_blueprint(recommend_query_bp)
    search_query_bp = search_query_blueprint(app, siwa)
    app.register_blueprint(search_query_bp)
    return app


#传感器解译信息
class SensorTranslation(BaseModel):
    fResolution: Optional[str] = Field(None, title="分辨率")
    fSensorstr: Optional[str] = Field(None, title="传感器光谱")
    
    # fResolution: str = Field(...,title="分辨率")
    fSensor: str = Field(...,title="传感器")
    # fSensorstr: str = Field(...,title="传感器光谱")
    fnodeid: int = Field(...,title="卫星节点号")
    fIsshow: str = Field(...,title="是否展示")
    id: str = Field(...,title="卫星ID")


class QueryField(BaseModel): 
    alisaName: str = Field(...,title="传感器/采集时间/云量") 
    name: str = Field(...,title="分辨传感器/采集时间/云量") 
    queryValue: Optional[Union[List[str], List[int]]] = Field(None, title="传感器/采集时间/云量的具体值")
    queryValue: Union[List[str], List[int]] = Field(...,title="传感器/采集时间/云量的具体值") 
    type: str = Field(...,title="查询参量") 
    nodeId: str = Field(...,title="卫星节点") 

    #针对不同类型的QueryField，添加额外的字段 
    defaultValue: Optional[str] = None #传感器的默认值
    connectionStr: Optional[str] = None  #传感器的？ 云量的？
    minValue: Optional[str] = None #采集时间的限制
    maxValue: Optional[str] = None 
    defaultMinValue: Optional[str] = None 
    defaultMaxValue: Optional[str] = None

#查询的表格
class Table(BaseModel):
    tableName: str = Field(...,title="查询卫星对应表名")
    queryFieldsList: List[QueryField] = Field(...,title="表格内容")


class QueryBody(BaseModel):
    guid: str = Field(...,title="全局统一标识符")
    nodeId: str = Field(...,title="卫星节点号")
    nodeName: str = Field(...,title="卫星表名")
    geometryType: int = Field(...,title="地理数据类型")
    areaCode: str = Field(...,title="行政区划代码")
    wkt: str = Field(...,title="地理位置信息")
    queryStatus: int = Field(...,title="查询状态")
    wktStr: Optional[str] = None
    isExl: str = Field(...,title="是否为excel表查询")
    isNoWkt: int = Field(...,title="一直是1 注释写的无需wkt直接查询")
    pageSize: int = Field(...,title="当前页数据条数")
    currentPage: int = Field(...,title="当前页查询结果")
    queryType: str = Field(...,title="查询类型")
    intervalDays: int = Field(...,title="间隔日期")
    sensortranslations: List[SensorTranslation] = Field(...,title="所有卫星信息")
    tables: Optional[List[Table]] = None
    #单页查询的结果比整体查询的结果少了一个objType: str = Field(...,title="查询类型ZL WX ")


class QueryParam(BaseModel):
    F_DATANAME: str = Field(..., title="数据名称")
    F_DID: int = Field(..., title="？")
    F_SCENEROW: str = Field(..., title="景_ROW")
    F_LOCATION: float = Field(..., title="位置")
    F_PRODUCTID: int = Field(..., title="产品序列号")
    F_PRODUCTLEVEL: str = Field(..., title="产品级别")
    NODENAME: str = Field(...,title="卫星节点")
    F_CLOUDPERCENT: int = Field(...,title="云量")
    F_TABLENAME: str = Field(..., title="表名")
    F_DATATYPENAME: str = Field(..., title="产品类型")
    F_ORBITID: int = Field(..., title="轨道号")
    NODEID: str = Field(..., title="卫星ID")
    F_DATANAME: str = Field(..., title="卫星节点名称")
    WKTRESPONSE: str = Field(..., title="经纬度信息")
    F_PRODUCETIME: str = Field(..., title="入库时间")
    F_SENSORID: str = Field(..., title="传感器ID")
    F_DATASIZE: float = Field(..., title="数据大小？")
    F_RECEIVETIME: str = Field(..., title="采集时间")
    F_DATAID: int = Field(..., title="数据编号")
    F_SATELLITEID: str = Field(..., title="拍摄卫星")
    F_SCENEPATH: str = Field(..., title="景_PATH")
    RN: int = Field(..., title="所查数据序号")
    # F_SCENEID: int = Field(..., title="场景ID")

class QueryResponse(BaseModel): 
    total: int = Field(...,title="数据条数") 
    guid: str = Field(...,title="全局统一标识符") 
    pageList: List[QueryParam] = Field(...,title="查询返回的结果") 
    coverage: Optional[float] = None
    Field(...,title="推荐数据覆盖面积")
    decryptFlag:bool = Field(...,title="数据加密情况") 
    status: int = Field(...) 
    version: str = Field(...)

class totalQueryTable(BaseModel):
    TOTAL: float = Field(..., title="合并数据的面积")
    RN: int = Field(..., title="行号")
    SIZENUM: int = Field(..., title="推荐数据的条数")
    WKTRESPONSE: str = Field(..., title="合并数据的WKT格式")

class totalQueryData(BaseModel):
    total: int = Field(...,title="")
    guid: str = Field(...,title="全局唯一标识符")
    pageList: List[totalQueryTable] = Field(...,title="合并面的返回结果")

class totalQueryResponse(BaseModel):
    #total: int = Field(...,title="数据条数")
    #guid: str = Field(...,title="全局统一标识符")
    #pageList: list[totalQueryTable] = Field(...,title="合并面返回的结果")
    data: totalQueryData = Field(...,title="")
    decryptFlag:bool = Field(...,title="数据加密情况") 
    status: int = Field(...) 
    version: str = Field(...)
    

def recommend_query_blueprint(app, siwa):
    recommend_query_bp = Blueprint('recommend_query_bp', __name__, url_prefix='/recommend_query')  
    @recommend_query_bp.post('/recommend')
    @siwa.doc(
        description='空间查询接口，用于"一键推荐"服务',
        summary="空间查询接口",
        body=QueryBody,
        resp=QueryResponse
    )
    
    def recommend_query():
#query: QueryBody, resp: QueryResponse
        json_data = request.get_json()

        query = QueryBody(**json_data)

        try:
            query = QueryBody(**json_data)
        except ValidationError as e:
            return {"error": e.errors()}, 400

        # 提取查询参数
        area_code = query.areaCode
        wkt = query.wkt
        if area_code == "" and wkt == "":
            area_code = "156000000"
            wkt = None
        else:
            if area_code == "":
                area_code = None
            if wkt == "":
                wkt = None
        table_name = query.nodeName.split(',')
        
        guid = query.guid
        pageSize = query.pageSize
        page = query.currentPage

        recommend_data , coverage_ratio = cacheFetchRecommendData(table_name, wkt, area_code, g.MyPool, g.MyCacheManager,
                                                                  guid,page, pageSize)
        
        # recommend_data , coverage_ratio = recommendData(table_name, wkt, area_code, pool)

        print(coverage_ratio)

        query_response = QueryResponse(
            total=len(recommend_data),  
            guid=str(query.guid),  
            pageList=recommend_data,  
            coverage=coverage_ratio,
            decryptFlag=False,  
            status=200,  
            version="1.0"  
        )
        return query_response.dict()

    @recommend_query_bp.post('/recommend_merge')
    @siwa.doc(
        description='空间查询全覆盖面接口，用于"一键推荐"结果的合并',
        summary="空间查询管覆盖面接口",
        body=QueryBody,
        resp=totalQueryResponse
    )
    def recommend_query_coverage():
        json_data = request.get_json()

        query = QueryBody(**json_data)

        try:
            query = QueryBody(**json_data)
        except ValidationError as e:
            return {"error": e.errors()}, 400

        area_code = query.areaCode
        wkt = query.wkt
        if area_code == "" and wkt == "":
            area_code = "156000000"
            wkt = None
        else:
            if area_code == "":
                area_code = None
            if wkt == "":
                wkt = None
        table_name = query.nodeName.split(',')
        guid = query.guid

        # pool = create_pool()
        sizenum, wktresponse,total,rn = cacheFeachRecomCoverData(table_name, wkt ,area_code,
                                                      g.MyCacheManager, guid, g.MyPool)
        recommend_coverage = {
            "SIZENUM" : sizenum,
            "WKTRESPONSE" : wktresponse,
            "TOTAL" : total,
            "RN" : rn
        }
        
        data = totalQueryData(
            total = sizenum,
            guid = query.guid,
            pageList = [recommend_coverage]
        )

        query_response = totalQueryResponse(
            data = data,  
            total = 0,
            decryptFlag=False,  
            status=200,  
            version="1.0"  
        )
        return query_response.dict()

    return recommend_query_bp


def search_query_blueprint(app, siwa):
    search_query_bp = Blueprint('search_query_bp', __name__, url_prefix='/search_query')

    @search_query_bp.post('/search')
    @siwa.doc(
        description='空间查询接口，用于"查询数据"服务',
        summary="空间查询接口",
        body=QueryBody,
        resp=QueryResponse
    )
    
    def search_query():
        json_data = request.get_json()
        query = QueryBody(**json_data)

        try:
            query = QueryBody(**json_data)
        except ValidationError as e:
            return {"error": e.errors()}, 400

        # 提取查询参数
        # pool = create_pool()
        area_code = query.areaCode  
        wkt = query.wkt
        if area_code == "" and wkt == "":
            area_code = "156000000"
            wkt = None  
        else:
            if area_code == "":
                area_code = None
            if wkt == "":
                wkt = None
        table_name = query.nodeName.split(',')

        for table in query.tables:
            for query_field in table.queryFieldsList:
                if query_field.alisaName == "云量":
                    cloud_percent_values = int(query_field.queryValue[0])
                if query_field.alisaName == "采集时间":
                    start_time_values = query_field.queryValue[0]
                    end_time_values = query_field.queryValue[1]


        search_data = searchData(table_name, wkt, area_code, start_time_values, end_time_values, cloud_percent_values, g.MyPool)

        query_response = QueryResponse(
            total=len(search_data),  
            guid=str(query.guid),  
            pageList=search_data,
            decryptFlag=False,  
            status=200,  
            version="1.0"  
        )
        return query_response.dict(exclude_none=True)

    return search_query_bp