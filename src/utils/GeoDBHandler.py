import geopandas as gpd
import shapely
from src.config import config
from src.utils.logger import logger

class DBHandler:
    def __init__(self, pool):
        self.pool = pool
        self.crs = config.CRS
        
    def ExecuteQuery(self, sql, pamars=None):
        """
        用以执行查询语句;
        sql: 查询语句;
        pamars: 查询参数;
        返回值为除最后一列以外的所有列名,以及查询结果;
        """
        try:
            with self.pool.acquire() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, pamars)
                    columns = [desc[0] for desc in cur.description[:-1]]
                    data = cur.fetchall()
                    return columns, data
        except Exception as e:
            logger.error(f"执行查询语句时出现错误: {e}, sql: {sql}, params: {pamars}")
            return [], []
        
        
class GeoDBHandler:
    def __init__(self):
        self.crs = config.CRS
        
    def dbDataToGeoDataFrame(self, rows, columns) -> gpd.GeoDataFrame:
        """
        将数据库查询结果转换为GeoDataFrame;
        rows: 数据库查询结果;
        columns: 除几何数据外的列名;
        默认最后一列为SDO_GEOMETRY对象;
        """
        geometries = []
        attributes = []
        for row in rows:
            # 分离几何和属性
            geometry = self.sdoGeometryToShapely(row[-1])
            attribute = row[:-1]
            geometries.append(geometry)
            attributes.append(attribute)
        
        # 创建GeoDataFrame
        gdf = gpd.GeoDataFrame(attributes, columns=columns, geometry=geometries, crs=self.crs)
        return gdf
    
    def sdoGeometryToShapely(self, sdo_geometry):    
        """
        将SDO_GEOMETRY对象转换为shapely几何对象。
        sdo_geometry: SDO_GEOMETRY对象(从数据库中直接读取);
        """
        if sdo_geometry is None:
            return None

        # 获取SDO_GTYPE, SDO_SRID, SDO_POINT, SDO_ELEM_INFO, SDO_ORDINATES
        sdo_gtype = sdo_geometry.SDO_GTYPE
        sdo_ordinates = sdo_geometry.SDO_ORDINATES.aslist()

        # 根据SDO_GTYPE确定几何类型
        if sdo_gtype == 2001:  # 点
            point = shapely.geometry.Point(sdo_ordinates)
            return point
        elif sdo_gtype == 2003:  # 多边形
            polygon = shapely.geometry.Polygon(self.pairwise(sdo_ordinates))
            return polygon
        # 可以根据需要处理其他几何类型
        else:
            logger.error(f"Unsupported SDO_GTYPE: {sdo_gtype}")
            return None
        
    def pairwise(self, iterable):
        """
        将可迭代对象两两配对;
        [long_1, lat_1, long_2, lat_2 ... long_5, lat_5] -> [(long_1, lat_1), (long_2, lat_2)...]
        """
        return [(iterable[i], iterable[i + 1]) for i in range(0, len(iterable)-1, 2)]