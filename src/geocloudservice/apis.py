from flask import Flask, request, jsonify, Blueprint
from flask_siwadoc import SiwaDoc
from waitress import serve
import oracledb
import json
from flask_cors import CORS
from marshmallow import ValidationError
from marshmallow import Schema, fields


# from src.utils.db.oracle import create_pool

from src.geocloudservice.blueprints.spatial_query_bp import spatial_query_blueprint
from src.geocloudservice.blueprints.recommend_query_bp import search_query_blueprint, recommend_query_blueprint
from src.geocloudservice.api_models import TimespanQueryModel
import src.config.config_template as config
from src.geocloudservice.blueprints.subscribe import subscribe_blueprint

from ..utils.db.oracle import create_pool, executeQueryAsDict


def gen_app():
    app = Flask(__name__,)
    CORS(app)
    siwa = SiwaDoc(app, title="FJY API", description="地质云航遥节点遥感数据服务系统接口文档")
    #cmm20241022用户订单反馈
    # pool = create_pool()

    # spatial_query_bp = spatial_query_blueprint(siwa, pool)
    # app.register_blueprint(spatial_query_bp)

    recommend_query_bp = recommend_query_blueprint(app, siwa)
    app.register_blueprint(recommend_query_bp)
    search_query_bp = search_query_blueprint(app, siwa)
    app.register_blueprint(search_query_bp)
    
    app.register_blueprint(app_get_areas_api(app, siwa))


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
    # app.register_blueprint(bp_feedback)
    # app.register_blueprint(bp_stat)
    return app


# def bp_stats(app, siwa):
#     bp_stats = Blueprint("stats", __name__, url_prefix='/stats')
#     @bp_stats.get("/daily")
#     @siwa.doc(
#         summary="每日统计",
#         description="每日统计",
#         tags=["stats"],
#     )
#     def daily():
#         return jsonify({"code": 200, "msg": "success", "data": "data"})

#     @bp_stats.get("/monthly")
#     @siwa.doc(
#         summary="每月统计",
#         description="每月统计",
#         tags=["stats"],
#     )
#     def monthly():
#         return jsonify({"code": 200, "msg": "success"})

#     @bp_stats.get("")
#     @siwa.doc(
#         summary="按时间统计接口",
#         description="按时间统计",
#         tags=["stats"],
#         query=TimespanQueryModel
#     )
#     def stats():
#         return jsonify({"code": 200, "msg": "success"})

#     app.register_blueprint(bp_stats)


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
                connection = oracledb.connect(user="jgf_gxfw", password="icw3kx45", dsn="10.82.8.4:1521/satdb")

                cursor = connection.cursor()
                cursor.execute("SELECT COUNT(*) FROM TF_ORDER WHERE F_STATUS NOT IN (-1, 0, 2, 5) AND F_CREATTIME BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-09-28 23:59:59'")
                user = cursor.fetchone()
                print(f"连接1成功，当前用户: {user[0]}")  # 打印当前用户以验证连接

                # 更新 TF_ORDER 表
                update_query = """
                UPDATE TF_ORDER
                SET F_FEEDBACK = :feedback, F_APPRAISE = :appraise
                WHERE F_ORDERNAME = :ordername
                """
                cursor.execute(update_query, feedback=content, appraise=score,ordername=ordername)
                
                # 提交更改
                connection.commit()

                return jsonify({'success': True, 'message': '反馈提交成功'})

            except oracledb.DatabaseError as e:
                error, = e.args
                return jsonify({'success': False, 'message': f'数据库错误: {error.message}'}), 500

            finally:
                # 关闭数据库连接
                if cursor:
                    cursor.close()
                if connection:
                    connection.close()

    app.register_blueprint(bp_feedback)

# cmm20241023已完成订单统计优化
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
            print(f"前端收到用户的查询：起始时间{start_time}，结束时间{end_time}")
                # ... 其他代码

            print('{lessCreattimeStr}', start_time, end_time)
            print(f"接收到的请求体: {data}")
            print(f"前端收到用户的查询：起始时间{start_time}，结束时间{end_time}")
        except ValidationError as err:
            return jsonify(err.messages), 400
        

        

        # if not start_time or not end_time:
        #     return jsonify({'success': False, 'message': '时间不能为空'}), 400

        try:
            connection = oracledb.connect(user="jgf_gxfw", password="icw3kx45", dsn="10.82.8.4:1521/satdb")
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM TF_ORDER WHERE F_STATUS NOT IN (-1, 0, 2, 5) AND F_CREATTIME BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-09-28 23:59:59'")
            user = cursor.fetchone()
            print(f"连接成功，当前用户: {user[0]}")  # 打印当前用户以验证连接

            # 使用参数化查询，确保日期格式正确
            query1 = "SELECT COUNT(*) FROM TF_ORDER WHERE F_STATUS=6 AND F_CREATTIME BETWEEN TO_TIMESTAMP(:start_time, 'YYYY-MM-DD HH24:MI:SS') AND TO_TIMESTAMP(:end_time, 'YYYY-MM-DD HH24:MI:SS') AND F_GET_METHOD = :F_GET_METHOD"
            
            #离线订单数量offline_order_num
            cursor.execute(query1, start_time=start_time, end_time=end_time, F_GET_METHOD='线下拷贝')
            offline_order_num = cursor.fetchone()[0]

            #在线订单数量online_order_num
            cursor.execute(query1, start_time=start_time, end_time=end_time, F_GET_METHOD='在线下载')
            online_order_num = cursor.fetchone()[0]

            #离线订单数据量offline_order_size
            query2="SELECT F_DATA_SUM FROM TF_ORDER WHERE F_STATUS=6 AND F_CREATTIME BETWEEN TO_TIMESTAMP(:start_time, 'YYYY-MM-DD HH24:MI:SS') AND TO_TIMESTAMP(:end_time, 'YYYY-MM-DD HH24:MI:SS') AND F_GET_METHOD = :F_GET_METHOD"
            cursor.execute(query2, start_time=start_time, end_time=end_time, F_GET_METHOD='线下拷贝')
            print(cursor.fetchone())
            results=cursor.fetchall()
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
            print("离线订单总数据量：", total_sum)
            offline_order_size = round(total_sum, 0)

            #在线订单数据量online_order_size
            cursor.execute(query2, start_time=start_time, end_time=end_time, F_GET_METHOD='在线下载')
            print(cursor.fetchone())
            results=cursor.fetchall()
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
            print("在线订单总数据量：", total_sum)
            online_order_size = round(total_sum, 0)

            #离线订单景数offline_order_scene_num
            query3="SELECT SUM(F_DATACOUNT) FROM TF_ORDER WHERE F_STATUS=6 AND F_CREATTIME BETWEEN TO_TIMESTAMP(:start_time, 'YYYY-MM-DD HH24:MI:SS') AND TO_TIMESTAMP(:end_time, 'YYYY-MM-DD HH24:MI:SS') AND F_GET_METHOD = :F_GET_METHOD"
            cursor.execute(query3, start_time=start_time, end_time=end_time, F_GET_METHOD='线下拷贝')
            offline_order_scene_num = cursor.fetchone()[0]

            #在线订单景数online_order_scene_num
            cursor.execute(query3, start_time=start_time, end_time=end_time, F_GET_METHOD='在线下载')
            online_order_scene_num = cursor.fetchone()[0]


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
        # except oracledb.DatabaseError as e:
        #     error, = e.args
        #     return jsonify({'success': False, 'message': f'数据库错误: {error.message}'}), 500


        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        

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
            connection = oracledb.connect(user="jgf_gxfw", password="icw3kx45", dsn="10.82.8.4:1521/satdb")
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
            connection = oracledb.connect(user="jgf_gxfw", password="icw3kx45", dsn="10.82.8.4:1521/satdb")
            cursor = connection.cursor()
            cursor.execute("""
                SELECT id, satellites_name, image_url, description 
                FROM satellitesinfo
                WHERE satellites_name = :name
            """, name=name)

            row = cursor.fetchone()

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

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    app.register_blueprint(product_intro)
    app.register_blueprint(subscribe_blueprint(app, siwa))


def app_get_areas_api(app, siwa):
    get_areas_bp = Blueprint("get_areas", __name__, url_prefix="/mj/agrsArea")

    # 获取所有地区树形结构接口
    @get_areas_bp.route("/get", methods=["GET"])
    @siwa.doc(summary="获取所有地区树形结构接口", description="")
    def get_area():
        # 解析GET参数
        # GET http://gf.agrs.cn:443/mj/agrsArea/get?showWkt=false&code=000000&qType=0&showType=0&showSub=true&showAllSub=true
        code = request.args.get("code", default="000000")
        q_type = request.args.get("qType", default=0)
        show_wkt = request.args.get("showWkt", default=False)
        show_sub = request.args.get("showSub", default=False)
        show_all_sub = request.args.get("showAllSub", default=False)

        tree = get_ares_tree(code, q_type, show_wkt, show_sub, show_all_sub)
        if not tree:
            return app_response({"error": "未找到对应的地区信息"}, 500)
        return app_response(tree)

    def get_ares_tree(code, q_type, show_wkt, show_sub, show_all_sub):  # 参数均未使用
        pool = create_pool()
        sql = "SELECT f_name AS name,f_distcode AS code FROM tc_district"
        result = executeQueryAsDict(pool, sql)
        if not result:
            return None
        return build_tree(result)

    def build_tree(data: list[dict]):
        """
        将线性行政区划数据转换为树形结构
        :param data: 行政区数据列表，线性，各个元素是包含code和name字段的字典
        :return: 树形结构数据
        """
        data = [
            {"code": item["CODE"].removeprefix("156"), "name": item["NAME"]}  # removeprefix 需要Python 3.9
            for item in data
        ]
        data.sort(key=lambda x: x["code"])
        if data[0]["code"] == "000000":  # 去除全国'000000'节点避免问题
            data.pop(0)

        nodes = {
            item["code"]: (
                {"code": item["code"], "name": item["name"], "child": []}
                if item["code"].endswith("00")
                else {"code": item["code"], "name": item["name"]}
            )
            for item in data
        }

        # 连接父子关系
        prov_list = []
        for item in data:
            area_code = item["code"]
            prov_code = area_code[:2] + "0000"  # 获取省级节点代码
            if area_code == prov_code:  # 判断是否为省级节点
                prov_list.append(nodes[area_code])
            else:
                city_code = area_code[:4] + "00"  # 获取市级节点代码
                if area_code == city_code:  # 判断是否为市级节点
                    nodes[prov_code]["child"].append(nodes[area_code])
                else:
                    if city_code not in nodes:
                        # print(f"未找到{item}的父节点{city_code}")
                        continue
                    nodes[city_code]["child"].append(nodes[area_code])

        return [{"code": "000000", "name": "全国", "child": prov_list}]

    return get_areas_bp


# app端的响应似乎都遵循这个格式，抽离出来
def app_response(data: dict, status_code: int = 200):
    DECRYPT_FLAG = False  # 两个变量临时放这儿
    VERSION = "v0.1.0-alpha1"
    response = {
        "data": data,
        "decryptFlag": DECRYPT_FLAG,
        "status": status_code,
        "version": VERSION,
    }
    return jsonify(response), status_code
