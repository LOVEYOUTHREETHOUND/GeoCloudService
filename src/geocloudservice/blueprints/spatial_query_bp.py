from flask import Blueprint, request
from pydantic import BaseModel, Field, HttpUrl
from typing import List, Optional, Tuple, Dict, Any
import datetime

from src.geocloudservice.spatial_computing import find_data_by_satellite, wkt_to_geometry, intersection_and_area

class BasicResponse(BaseModel):
    links: Dict[str, HttpUrl]  # HATEOAS links
    data: Dict[str, Any]

class SpatialQueryParam(BaseModel):
    satellite_names: List[str] = Field(..., description="卫星名称列表")
    wkt: str = Field(..., description="WKT格式的空间查询条件")


class QueryResponse(BasicResponse):
    dataname: list


def spatial_query_blueprint(siwa, pool):
    spatial_query_bp = Blueprint('spatial_query_bp', __name__, url_prefix='/spatial_query')

    @spatial_query_bp.get('/spatial')
    @siwa.doc(
        description='空间查询接口，用于"一键推荐"服务',
        summary="空间查询接口",
        query=SpatialQueryParam,
        resp=QueryResponse
    )
    def spatial_query():
        query_wkt = request.args.get('wkt')
        satellite_names = request.args.get('satellite_names')

        # 1. Fetch satellite data by name
        data_df = find_data_by_satellite(satellite_names, pool)

        # 2. Convert WKT to geometry
        geometry = wkt_to_geometry(query_wkt)

        # 3. Get data that is intersected with the query geometry
        intersected_data = intersection_and_area(data_df, geometry)

        return {
            "dataname": intersected_data,
            "links": {
                "self": request.url
            }
        }
    return spatial_query_bp
