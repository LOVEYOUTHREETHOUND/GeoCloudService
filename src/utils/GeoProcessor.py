import geopandas as gpd
from shapely.geometry import Polygon
import shapely
from src.config import config


class GeoProcessor:
    def __init__(self):
        self.crs = config.CRS
   
    def wktToShapely(self, wkt: str) -> gpd.GeoDataFrame:
        """
        将wkt转成Shapeley对象;
        wkt: WKT格式的几何形状;
        """
        geometry = shapely.wkt.loads(wkt)
        return geometry
    
    def findIntersectedData(self, target_area, data_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        查找与目标区域相交的数据;
        target_area: 目标区域,一般为shapely.geometry.Polygon对象;
        data_gdf: 数据GeoDataFrame;
        数据要求: data_gdf中至少有一个geometry列,且该列储存的是shapely.geometry.Polygon对象;
        """
        try:
            # 检查每个几何形状是否与target_area相交
            intersects = data_gdf['geometry'].intersects(target_area, align=True)
            
            # 使用布尔索引来筛选出相交的数据
            intersecting_data_gdf = data_gdf[intersects].copy()
            return intersecting_data_gdf
        except Exception as e:
            print(f"检查相交数据时出现错误: {e}")
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
            coverage_ratio = intersection.area / target_area.area
            
            return coverage_ratio
        except Exception as e:
            print(f"计算覆盖率时出现错误: {e}")
            return 0.0

    def GeoDataFrameToDict(self, data: gpd.GeoDataFrame) -> dict:
        """
        将GeoDataFrame转换为字典;
        data: GeoDataFrame;
        返回值格式：[{'dataname': 'A', 'geometry': 'POLYGON ((...))'}, {'dataname': 'B', 'geometry': 'POLYGON ((...))'}, ...]
        """
        data_dict = data.to_dict(orient='records')
        return data_dict
    
    def GeoDataFrameToList(self, data: gpd.GeoDataFrame) -> list:
        """
        将GeoDataFrame转换为列表;
        data: GeoDataFrame;
        返回值格式：[['A', 'POLYGON ((...))'], ['B', 'POLYGON ((...))'], ...]
        """
        data_list = data.values.tolist()
        return data_list
 

