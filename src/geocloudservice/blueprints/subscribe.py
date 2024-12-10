from flask import Blueprint, request, Flask, g
from pydantic import BaseModel, Field, HttpUrl
from flask_cors import CORS
from flask_siwadoc import SiwaDoc
from typing import List, Optional, Tuple, Dict, Any, Union
from datetime import datetime

from src.utils.db.oracle import create_pool, executeQuery, executeNonQuery
import src.utils.logger as logger

class QueryField(BaseModel): 
    alisaName: str = Field(...,title="传感器/采集时间/云量") 
    name: str = Field(...,title="分辨传感器/采集时间/云量") 
    queryValue: Optional[Union[List[str], List[int]]] = Field(None, title="传感器/采集时间/云量的具体值")

class Table(BaseModel):
    tableName: str = Field(...,title="查询卫星对应表名")
    queryFieldsList: List[QueryField] = Field(...,title="表格内容")


class SubscribeRequest(BaseModel):
    loginName: str = Field(..., description="登录名")
    areaCode: str = Field(..., description="区域编码")
    wkt: str = Field(..., description="WKT格式区域")
    isNoWkt: str = Field(..., description="是否WKT格式")
    nodeName: str = Field(..., description="数据库表名列表")
    tables: Optional[List[Table]] = None

def subscribe_blueprint(app: Flask, siwa: SiwaDoc) -> Blueprint:
    subscribe = Blueprint("subscribe", __name__)
    
    @siwa.doc(
        description="订阅功能接口",   
        body=SubscribeRequest
    )
    @subscribe.route("/agrsQueryModuleSpatial/sendSubscribeRequest", methods=["POST"])
    def subscribeService():
        try:
            json_data = request.get_json()
            query = SubscribeRequest(**json_data)
            
            pool = g.MyPool
            userId, areaCode, wkt, isWKT, nodeNames, cloudPercent, subStartTime, subEndTime = validateSubscribeRequest(query,pool)

            subTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 时间格式转化
            subTime = formatTime(subTime)
            subStartTime = formatTime(subStartTime)
            subEndTime = formatTime(subEndTime)
            
            # 生成订阅ID
            subID = generateSubID(pool)
            
            # 插入订阅信息
            insertSubscribe(pool, subID, userId, areaCode, wkt, isWKT, nodeNames, cloudPercent, subTime, subStartTime, subEndTime, "0")
            return {"subID": subID}
        except Exception as e:
            logger.error(f"订阅失败: {e}")
            return {"subID": None}
        
    return subscribe

def formatTime(time: str) -> str:
    try:
        time = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
        return time.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.error(f"时间格式转化失败: {e}")
        return None
    
def generateSubID(pool) -> str:
    sql = "select SUBID from SUBSCRIBE_ORDER where SUBID is not null order by SUBID desc fetch first 1 rows only"
    newestSubID = executeQuery(pool, sql)
    currentDate = datetime.now().strftime("%Y%m%d")
    
    # 检查日期
    if len(newestSubID) != 0:
        sequenctPart = 1
    else:
        newestSubID = newestSubID[0][0]
        dataPart = newestSubID[:8]
        sequenctPart = int(newestSubID[10:])   
        if currentDate == dataPart:
            sequenctPart = 1
        else:
            sequenctPart += 1
    formatSequence = str(sequenctPart).zfill(5)
    subID = f"{currentDate}DY{formatSequence}"
    return subID

def getUserIdByLoginName(pool, loginName: str) -> str:
    sql = "select F_ID from TC_SYS_USER where F_LOGINNAME = :loginName"
    parames = {'loginName': loginName}
    userId = executeQuery(pool, sql, parames)
    if len(userId) == 0:
        raise Exception("登录名错误，此用户不存在")
    else:
        return userId[0][0]


def insertSubscribe(pool, subID: str, userId: str, areaCode: str, wkt: str, isWKT: str, nodeNames: str, cloudPercent: float, subTime: str, subStartTime: str, subEndTime: str, status: str) -> bool:
    sql = "insert into SUBSCRIBE_ORDER(SUBID, USERID, AREACODE, WKT, ISWKT, NODENAMES, CLOUDPERCENT, SUBTIME, SUBSTARTTIME, SUBENDTIME, STATUS)\
        values(:subID, :userId, :areaCode, :wkt, :isWKT, :nodeNames, :cloudPercent, \
            TO_DATE(:subTime, 'YYYY-MM-DD HH24:MI:SS'), TO_DATE(:subStartTime, 'YYYY-MM-DD HH24:MI:SS'),\
                TO_DATE(:subEndTime, 'YYYY-MM-DD HH24:MI:SS'), :status)"
    parames = {'subID': subID, 'userId': userId, 'areaCode': areaCode, 'wkt': wkt, 
               'isWKT': isWKT, 'nodeNames': nodeNames, 'cloudPercent': cloudPercent, 'subTime': subTime, 
               'subStartTime': subStartTime, 'subEndTime': subEndTime, 'status': status}
    executeNonQuery(pool, sql, parames)
 
def validateSubscribeRequest(request: SubscribeRequest, pool):
        LoginName = request.loginName
        userId = getUserIdByLoginName(pool, LoginName)
        
        areaCode = request.areaCode
        wkt = request.wkt
        if areaCode == "" and wkt == "":
            raise Exception("areaCode和WKT不能同时为空")
        elif areaCode == "":
            isWKT = 1
            areaCode = None
        elif wkt == "":
            isWKT = 0
            wkt = None
        nodeNames = request.nodeName
        tables = request.tables  
        for table in tables:
            for queryfield in table.queryFieldsList:
                if queryfield.alisaName == "云量":
                    cloudPercent = int(queryfield.queryValue[0])
                if queryfield.alisaName == "采集时间":
                    subStartTime = queryfield.queryValue[0]
                    subEndTime = queryfield.queryValue[1]
        return userId, areaCode, wkt, isWKT, nodeNames, cloudPercent, subStartTime, subEndTime 