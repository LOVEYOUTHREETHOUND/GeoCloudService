import geopandas as gpd
from src.config import config
from shapely.geometry import Point, LineString, Polygon, box
from src.utils.logger import logger

class GeoProcessor:
    def __init__(self):
        self.crs = config.CRS
   
    def findIntersectedData(self, target_area, data_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        查找与目标区域相交的数据;
        target_area: 目标区域,一般为shapely.geometry对象;
        data_gdf: 数据GeoDataFrame;
        数据要求: data_gdf中至少有一个geometry列,且该列储存的是shapely.geometry对象;
        返回值: 与目标区域相交的数据GeoDataFrame;
        """
        try:
            # 检查每个几何形状是否与target_area相交
            intersects = data_gdf['geometry'].intersects(target_area, align=True)
            
            # 使用布尔索引来筛选出相交的数据
            intersecting_data_gdf = data_gdf[intersects].copy()
            return intersecting_data_gdf
        except Exception as e:
            logger.error(f"检查相交数据时出现错误: {e}")
            return gpd.GeoDataFrame()
        
    def calCoverageRatio(self,target_area, data_gdf: gpd.GeoDataFrame) -> float:
        """
        计算数据覆盖率;
        target_area: 目标区域,一般为shapely.geometry.Polygon对象;
        data_gdf: 数据GeoDataFrame;
        数据要求: data_gdf中至少有一个geometry列,且该列储存的是shapely.geometry.Polygon对象;
        """
        try:
            # 合并data_gdf中的几何形状
            combined_data = data_gdf['geometry'].unary_union
            
            # 计算相交区域
            intersection = combined_data.intersection(target_area)
            
            # 计算覆盖率
            coverage_ratio = 0

            if intersection.is_empty:
                return 0.0
            
            if isinstance(target_area, Point):
                coverage_ratio = 1
            elif isinstance(target_area, LineString):
                coverage_ratio = intersection.length / target_area.length
            else:
                coverage_ratio = intersection.area / target_area.area
            
            return coverage_ratio
        except Exception as e:
            logger.error(f"计算覆盖率时出现错误: {e}")
            return 0.0

    def getEnvelope(self, data: gpd.GeoDataFrame) -> Polygon:
        """
        获取数据的外接矩形;
        data: GeoDataFrame;
        返回值: 外接矩形,一般为shapely.geometry.Polygon对象;
        """
        try:
            
            minx, miny, maxx, maxy = data.total_bounds
            return box(minx, miny, maxx, maxy)
        except Exception as e:
            logger.error(f"获取外接矩形时出现错误: {e}")
            return None
        
    def getCoordinateRange(self, geom) -> tuple:
        """
        获取几何形状的坐标范围;
        geom: shapely.geometry对象;
        返回值: (minx, maxx, miny, maxy);
        """
        try:
            minx, miny, maxx, maxy = geom.bounds
            return (minx, maxx, miny, maxy)
        except Exception as e:
            logger.error(f"获取坐标范围时出现错误: {e}")
            return None

    def GeoDataFrameToDict(self, data: gpd.GeoDataFrame) -> dict:
        """
        将GeoDataFrame转换为字典;
        data: GeoDataFrame;
        返回值格式：[{'dataname': 'A', 'geometry': 'POLYGON ((...))'}, {'dataname': 'B', 'geometry': 'POLYGON ((...))'}, ...]
        """
        result_list = []
        for _, row in data.iterrows():
            row_dict = {}
            for key, value in row.items():
                if key == 'geometry':
                    row_dict[key] = value.wkt
                else:
                    row_dict[key] = str(value)
            result_list.append(row_dict)
        return result_list

    def GeoDataFrameToList(self, data: gpd.GeoDataFrame) -> list:
        """
        将GeoDataFrame转换为列表;
        data: GeoDataFrame;
        返回值格式：[['A', 'POLYGON ((...))'], ['B', 'POLYGON ((...))'], ...]
        """
        result_list = []
        for _, row in data.iterrows():
            wkt = row['geometry'].wkt
            other_values = [str(value) for key, value in row.items() if key != 'geometry']
            result_list.append(other_values + [wkt])
        
        return result_list

