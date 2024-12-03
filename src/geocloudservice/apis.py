from flask import Flask, request, jsonify, Blueprint, Response
from flask_siwadoc import SiwaDoc
from waitress import serve
import oracledb
import json
from flask_cors import CORS
from marshmallow import ValidationError
from marshmallow import Schema, fields
import requests
import minio 
from minio.error import S3Error
from src.utils.db.minIO import create_minio_client
from src.utils.logger import logger
from urllib.parse import quote

from src.utils.CacheManager import CacheManager, SimpleCache
from flask import g
from src.utils.db.oracle import create_pool, executeQuery, executeNonQuery

from src.geocloudservice.blueprints.spatial_query_bp import spatial_query_blueprint
from src.geocloudservice.blueprints.recommend_query_bp import search_query_blueprint, recommend_query_blueprint
from src.geocloudservice.api_models import TimespanQueryModel
from src.geocloudservice.blueprints.subscribe import subscribe_blueprint




def gen_app():
    app = Flask(__name__,)
    CORS(app)
    siwa = SiwaDoc(app, title="FJY API", description="地质云航遥节点遥感数据服务系统接口文档")

    MyPool = create_pool()
    cache = SimpleCache()
    MyCacheManager = CacheManager(cache)
    
    @app.before_request
    def loadParams():
        g.MyPool = MyPool
        g.MyCacheManager = MyCacheManager

    recommend_query_bp = recommend_query_blueprint(app, siwa)
    app.register_blueprint(recommend_query_bp)
    search_query_bp = search_query_blueprint(app, siwa)
    app.register_blueprint(search_query_bp)
    subscribe_blueprint_bp = subscribe_blueprint(app, siwa)
    app.register_blueprint(subscribe_blueprint_bp)

    @app.post(f"/test")
    @siwa.doc(
        summary="测试接口",
        description="测试接口",
        tags=["test"],
    )
    def test():
        return jsonify({"code": 200, "msg": "successsss"})
    # bp_stats(app, siwa)
    #cmm20241012用户订单反馈接口
    bp_feedback(app, siwa)
    #cmm20241023已完成订单统计优化
    bp_stat(app, siwa)
    product_intro(app, siwa)
    user_guide(app,siwa)
    # app.register_blueprint(bp_feedback)
    # app.register_blueprint(bp_stat)
    return app
    

# cmm20241012用户订单反馈接口
def bp_feedback(app, siwa):
    bp_feedback = Blueprint("feedback", __name__, url_prefix='/bupt_feedback')
    @bp_feedback.post("/submit")# /feedback/20240926WP00001
    @siwa.doc(
        summary="订单反馈",
        description="",
        tags=["feedback"],
    )
    def submit_feedback():
        # 这里添加处理反馈的逻辑
        # 例如，从请求体中获取数据，保存到数据库等
        # 假设我们只是返回一个成功的响应
            data = request.get_json()
            score = data.get('score')
            content = data.get('content')
            ordername = data.get('ordername')
            print(f"收到订单 {ordername} 的反馈，评分 {score}，内容 {content}")

            if not score:
                return jsonify({'success': False, 'message': '评分不能为空'}), 400

            try:
                # 连接到 Oracle 数据库
                # connection = oracledb.connect(user="JGF_GXFW", password="JGF_GXFW", dsn="62.234.192.247:18881/ORCLCDB")
                # connection = oracledb.connect(user="jgf_gxfw", password="icw3kx45", dsn="10.82.8.4:1521/satdb")

                # cursor = connection.cursor()
                # cursor.execute("SELECT COUNT(*) FROM TF_ORDER WHERE F_STATUS NOT IN (-1, 0, 2, 5) AND F_CREATTIME BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-09-28 23:59:59'")
                # user = cursor.fetchone()
                
                selectSql = "SELECT COUNT(*) FROM TF_ORDER WHERE F_STATUS NOT IN (-1, 0, 2, 5) AND \
                    F_CREATTIME BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-09-28 23:59:59'"
                user = executeQuery(g.MyPool, selectSql)
                print(f"连接1成功,当前用户: {user[0]}")  # 打印当前用户以验证连接

                # 更新 TF_ORDER 表
                update_query = """
                UPDATE TF_ORDER
                SET F_FEEDBACK = :feedback, F_APPRAISE = :appraise
                WHERE F_ORDERNAME = :ordername
                """
                executeNonQuery(g.MyPool, update_query, {'feedback': content, 'appraise': score, 'ordername': ordername})
                # cursor.execute(update_query, feedback=content, appraise=score,ordername=ordername)
                
                # 提交更改
                # connection.commit()

                return jsonify({'success': True, 'message': '反馈提交成功'})

            except oracledb.DatabaseError as e:
                error, = e.args
                return jsonify({'success': False, 'message': f'数据库错误: {error.message}'}), 500

            # finally:
            #     # 关闭数据库连接
            #     if cursor:
            #         cursor.close()
            #     if connection:
            #         connection.close()

    app.register_blueprint(bp_feedback)

#cmm20241023已完成订单统计优化
def bp_stat(app, siwa):
    bp_stat = Blueprint("stat", __name__, url_prefix='/bupt_stat')
    @bp_stat.post("/get")
    @siwa.doc(
        summary="已完成订单统计",
        description="",
        tags=["stat"],
        body=TimespanQueryModel 
    )
    def get_stat():
        try:
            # data = request.get_json()
            data = json.loads(request.get_data())
            print('data:', data)
            # time_list=TimespanQueryModel(**data)
            time_list = TimespanQueryModel.parse_obj(data["data"])
            print('time_list:', time_list)
            start_time = time_list.lessCreattimeStr
            end_time = time_list.moreCreattimeStr
                # 校验数据是否包含null值

            if start_time is None and end_time is not None:
                return {"error": "起始时间未填写！"}, 400  # 返回错误信息和HTTP状态码400
            if start_time is not None and end_time is None:
                return {"error": "结束时间未填写！"}, 400  # 返回错误信息和HTTP状态码400
            if start_time is None and end_time is None:
                return {"error": "起始时间与结束时间未填写！"}, 400  # 返回错误信息和HTTP状态码400
            
            if start_time is not None and end_time is not None and end_time < start_time:
                return {"error": "结束时间需晚于起始时间！"}, 400  # 返回错误信息和HTTP状态码400
            # inner_data = schema.load(data)  # 直接加载整个数据
            # print (inner_data)
            # start_time = inner_data.get('lessCreattimeStr')
            # end_time = inner_data.get('moreCreattimeStr')
            # print(f"接收到的请求体: {data}")
            # print(f"前端收到用户的查询：起始时间{start_time}，结束时间{end_time}")
            #     # ... 其他代码

            # print('{lessCreattimeStr}', start_time, end_time)
            # print(f"接收到的请求体: {data}")
            # print(f"前端收到用户的查询：起始时间{start_time}，结束时间{end_time}")
            logger.info(f"前端收到用户的查询：起始时间{start_time}，结束时间{end_time}")
            logger.info(f"接收到的请求体: {data}")
        except ValidationError as err:
            return jsonify(err.messages), 400
        

        

        # if not start_time or not end_time:
        #     return jsonify({'success': False, 'message': '时间不能为空'}), 400

        try:
            # connection = oracledb.connect(user="jgf_gxfw", password="icw3kx45", dsn="10.82.8.4:1521/satdb")
            # cursor = connection.cursor()
            # cursor.execute("SELECT COUNT(*) FROM TF_ORDER WHERE F_STATUS NOT IN (-1, 0, 2, 5) AND F_CREATTIME BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-09-28 23:59:59'")
            # user = cursor.fetchone()
            selectSql = "SELECT COUNT(*) FROM TF_ORDER WHERE F_STATUS NOT IN (-1, 0, 2, 5) AND \
                F_CREATTIME BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-09-28 23:59:59'"
            user = executeQuery(g.MyPool, selectSql)
            print(f"连接成功，当前用户: {user[0]}")  # 打印当前用户以验证连接

            # 使用参数化查询，确保日期格式正确
            query1 = "SELECT COUNT(*) FROM TF_ORDER WHERE F_STATUS=6 AND F_CREATTIME BETWEEN TO_TIMESTAMP(:start_time, 'YYYY-MM-DD HH24:MI:SS') \
                AND TO_TIMESTAMP(:end_time, 'YYYY-MM-DD HH24:MI:SS') AND F_GET_METHOD = :F_GET_METHOD"
            
            #离线订单数量offline_order_num
            # cursor.execute(query1, start_time=start_time, end_time=end_time, F_GET_METHOD='线下拷贝')
            # offline_order_num = cursor.fetchone()[0]
            offline_order_num = executeQuery(g.MyPool, query1, 
                                             {'start_time': start_time, 'end_time' : end_time, 'F_GET_METHOD': '线下拷贝'})[0]

            #在线订单数量online_order_num
            # cursor.execute(query1, start_time=start_time, end_time=end_time, F_GET_METHOD='在线下载')
            # online_order_num = cursor.fetchone()[0]
            online_order_num = executeQuery(g.MyPool, query1, 
                                             {'start_time': start_time, 'end_time' : end_time, 'F_GET_METHOD': '在线下载'})[0]

            #离线订单数据量offline_order_size
            query2="SELECT F_DATA_SUM FROM TF_ORDER WHERE F_STATUS=6 AND F_CREATTIME \
                BETWEEN TO_TIMESTAMP(:start_time, 'YYYY-MM-DD HH24:MI:SS') AND TO_TIMESTAMP(:end_time, 'YYYY-MM-DD HH24:MI:SS') AND F_GET_METHOD = :F_GET_METHOD"
            # cursor.execute(query2, start_time=start_time, end_time=end_time, F_GET_METHOD='线下拷贝')
            # print(cursor.fetchone())
            # results=cursor.fetchall()
            results = executeQuery(g.MyPool, query2, 
                                             {'start_time': start_time, 'end_time' : end_time, 'F_GET_METHOD': '线下拷贝'})
            # 初始化总和
            total_sum = 0
            # 处理每个结果
            for result in results:
                if result[0] is None:  # 检查是否为空值
                    total_sum += 0  # 默认加0
                    continue  # 跳过空值，继续下一个循环
                data_sum = result[0].strip()  # 去除空格
                if data_sum.endswith('M'):
                    data_sum = data_sum[:-1]  # 去除 'M'
                    total_sum += float(data_sum)
                elif data_sum.endswith('G'):
                    data_sum = data_sum[:-1]  # 去除 'G'
                    total_sum += float(data_sum) * 1024
                else:
                    total_sum = total_sum
            total_sum=total_sum/1024
            # print(results)
            # 打印总和
            # print("离线订单总数据量：", total_sum)
            logger.info(f"离线订单总数据量：{total_sum}")
            offline_order_size = round(total_sum, 0)

            #在线订单数据量online_order_size
            # cursor.execute(query2, start_time=start_time, end_time=end_time, F_GET_METHOD='在线下载')
            # print(cursor.fetchone())
            # results=cursor.fetchall()
            results = executeQuery(g.MyPool, query2,
                                  {'start_time': start_time, 'end_time': end_time, 'F_GET_METHOD': '在线下载'})
            # 初始化总和
            total_sum = 0
            # 处理每个结果
            for result in results:
                if result[0] is None:  # 检查是否为空值
                    total_sum += 0  # 默认加0
                    continue  # 跳过空值，继续下一个循环
                data_sum = result[0].strip()  # 去除空格
                if data_sum.endswith('M'):
                    data_sum = data_sum[:-1]  # 去除 'M'
                    total_sum += float(data_sum)
                elif data_sum.endswith('G'):
                    data_sum = data_sum[:-1]  # 去除 'G'
                    total_sum += float(data_sum) * 1024
                else:
                    total_sum = total_sum
            total_sum=total_sum/1024
            # print(results)
            # 打印总和
            # print("在线订单总数据量：", total_sum)
            logger.info(f"在线订单总数据量：{total_sum}")
            online_order_size = round(total_sum, 0)

            #离线订单景数offline_order_scene_num
            query3="SELECT SUM(F_DATACOUNT) FROM TF_ORDER WHERE F_STATUS=6 AND \
                    F_CREATTIME BETWEEN TO_TIMESTAMP(:start_time, 'YYYY-MM-DD HH24:MI:SS') AND TO_TIMESTAMP(:end_time, 'YYYY-MM-DD HH24:MI:SS') AND \
                    F_GET_METHOD = :F_GET_METHOD"
            # cursor.execute(query3, start_time=start_time, end_time=end_time, F_GET_METHOD='线下拷贝')
            # offline_order_scene_num = cursor.fetchone()[0]
            offline_order_scene_num = executeQuery(g.MyPool, query3,
                                        {'start_time': start_time, 'end_time': end_time, 'F_GET_METHOD': '线下拷贝'})[0]

            #在线订单景数online_order_scene_num
            # cursor.execute(query3, start_time=start_time, end_time=end_time, F_GET_METHOD='在线下载')
            # online_order_scene_num = cursor.fetchone()[0]
            online_order_scene_num = executeQuery(g.MyPool, query3,
                                        {'start_time': start_time, 'end_time': end_time, 'F_GET_METHOD': '在线下载'})[0]


               # 构建响应数据
            order_stats = {
                "offlineOrderNum": offline_order_num,
                "onlineOrderNum": online_order_num,
                "offlineOrderSize": offline_order_size,
                "onlineOrderSize": online_order_size,
                "offlineOrderSceneNum": offline_order_scene_num,
                "onlineOrderSceneNum": online_order_scene_num,
                "orderSize": offline_order_size + online_order_size  # 总数据量
                # "OrderSize": offline_order_size + online_order_size  # 总数据量
            }

            return jsonify(order_stats)
        except oracledb.DatabaseError as e:
            error, = e.args
            logger.error(f'数据库错误: {error.message}')
            return jsonify({'success': False, 'message': f'数据库错误: {error.message}'}), 500


        # finally:
        #     if cursor:
        #         cursor.close()
        #     if connection:
        #         connection.close()
        

    app.register_blueprint(bp_stat)

def product_intro(app,siwa):
    product_intro = Blueprint("productInfo", __name__, url_prefix='/productInfo')
    # 获取所有卫星名称接口
    @product_intro.route('/satellites', methods=['GET'])
    @siwa.doc(
        summary="获取所有卫星名称接口",
        description="获取所有卫星名称接口",
        tags=["productInfo"]
    )
    def get_satellites():
        try:
            # connection = oracledb.connect(user="jgf_gxfw", password="icw3kx45", dsn="10.82.8.4:1521/satdb")
            connection = g.MyPool.acquire()
            cursor = connection.cursor()
            cursor.execute("""
                SELECT id, satellites_name 
                FROM satellitesinfo
            """)
            satellites = []
            for row in cursor:
                satellites.append({
                    'id': row[0],
                    'name': row[1]
                })
            return jsonify(satellites)

        except oracledb.DatabaseError as e:
            error, = e.args
            return jsonify({"error": str(error)}), 500

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    # 通过卫星名获取对应卫星介绍接口
    @product_intro.route('/satellite/name', methods=['POST'])
    @siwa.doc(
        summary="获取卫星介绍接口",
        description="通过卫星名获取对应卫星介绍接口",
        tags=["productInfo"]
    )
    def post_satellite_by_name():
        try:
            data = request.get_json() 
            name = data.get('name')  

            if not name:
                return jsonify({"error": "未获取卫星名"}), 400
            # connection = oracledb.connect(user="jgf_gxfw", password="icw3kx45", dsn="10.82.8.4:1521/satdb")
            # cursor = connection.cursor()
            # cursor.execute("""
            #     SELECT id, satellites_name, image_url, description 
            #     FROM satellitesinfo
            #     WHERE satellites_name = :name
            # """, name=name)

            # row = cursor.fetchone()
            
            selectSql = """
                SELECT id, satellites_name, image_url, description 
                FROM satellitesinfo
                WHERE satellites_name = :name"""
            row = executeQuery(g.MyPool, selectSql, {'name': name})[0]

            if row:
                satellite = {
                    'id': row[0],
                    'name': row[1],
                    'imageUrl': row[2],
                    'description': str(row[3]) if row[3] else None
                }
                return jsonify(satellite)
            else:
                return jsonify({'error': '未找到对应的卫星'}), 404

        except oracledb.DatabaseError as e:
            error, = e.args
            return jsonify({"error": str(error)}), 500

        # finally:
        #     if cursor:
        #         cursor.close()
        #     if connection:
        #         connection.close()

    app.register_blueprint(product_intro)




def user_guide(app,siwa):
    user_guide = Blueprint("user_guide", __name__, url_prefix='/userGuide')
    # 获取操作指导视频链接接口
    @user_guide.route('/videourl', methods=['GET'])
    @siwa.doc(
        summary="获取操作指导视频链接接口",
        description="获取操作指导视频链接接口",
        tags=["userGuide"]
    )
    def get_video_url():
        title = request.args.get('title')
        video_resources = {
            "地质云遥感数据平台操作说明":"http://10.82.8.64:8080/satellite.pic/%E7%B3%BB%E7%BB%9F%E6%93%8D%E4%BD%9C%E6%BC%94%E7%A4%BA%E8%A7%86%E9%A2%91.mp4"
        }
        video_url = video_resources.get(title)
        if video_url:
            return jsonify({'hrefData': video_url})
        else:
            return jsonify({'error': '未找到对应的视频链接'}), 404

    # 获取操作指导视频下载接口
    @user_guide.route('/videodownload', methods=['GET'])
    @siwa.doc(
        summary="获取操作指导视频下载接口",
        description="获取操作指导视频下载接口",
        tags=["userGuide"]
    )
    def download_video():

        minio_client = create_minio_client()

        title = request.args.get('title')  # 从请求参数获取视频标题

        # 根据标题找到文件映射路径（可以根据需要动态生成或从配置中读取）
        minio_file_mapping = {
            "地质云遥感数据平台操作说明": "系统操作演示视频.mp4"
        }
        object_name = minio_file_mapping.get(title)

        bucket_name = "satellite.pic"

        if not object_name:
            return jsonify({"error": "视频文件不存在"}), 404

        try:
            # 获取文件流
            response = minio_client.get_object(bucket_name, object_name)
            # 获取文件大小
            stat = minio_client.stat_object(bucket_name, object_name)

            encoded_file_name = quote(f'{title}.mp4')

            # 生成流式响应
            return Response(
                response,  # 文件流
                headers={
                    "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_file_name}",  # 下载文件名
                    "Content-Type": "video/mp4",  # 设置视频文件类型
                    "Content-Length": str(stat.size),  # 文件大小
                },
                status=200,
            )
        except S3Error as e:
            logger.error(f"从 MinIO 下载视频文件失败: {e}, 文件名: {object_name}")
            return jsonify({"error": f"无法下载视频文件: {str(e)}"}), 500

    app.register_blueprint(user_guide)
