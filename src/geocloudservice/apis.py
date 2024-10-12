from flask import Flask, request, jsonify, Blueprint
from flask_siwadoc import SiwaDoc
from waitress import serve
import oracledb
from flask_cors import CORS


from src.geocloudservice.api_models import TimespanQueryModel
import src.config.config_template as config


def gen_app():
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
        return jsonify({"code": 200, "msg": "successsss"})
    bp_stats(app, siwa)
    bp_feedback(app, siwa)
    return app


def bp_stats(app, siwa):
    bp_stats = Blueprint("stats", __name__, url_prefix='/stats')
    @bp_stats.get("/daily")
    @siwa.doc(
        summary="每日统计",
        description="每日统计",
        tags=["stats"],
    )
    def daily():
        return jsonify({"code": 200, "msg": "success", "data": "data"})

    @bp_stats.get("/monthly")
    @siwa.doc(
        summary="每月统计",
        description="每月统计",
        tags=["stats"],
    )
    def monthly():
        return jsonify({"code": 200, "msg": "success"})

    @bp_stats.get("")
    @siwa.doc(
        summary="按时间统计接口",
        description="按时间统计",
        tags=["stats"],
        query=TimespanQueryModel
    )
    def stats():
        return jsonify({"code": 200, "msg": "success"})

    app.register_blueprint(bp_stats)
    

# cmm20241012用户订单反馈接口
def bp_feedback(app, siwa):
    bp_feedback = Blueprint("feedback", __name__, url_prefix='/feedback')
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
                connection = oracledb.connect(user="JGF_GXFW", password="JGF_GXFW", dsn="62.234.192.247:18881/ORCLCDB")
                cursor = connection.cursor()
                cursor.execute("SELECT COUNT(*) FROM TF_ORDER WHERE F_STATUS NOT IN (-1, 0, 2, 5) AND F_CREATTIME BETWEEN TIMESTAMP '2024-01-01 00:00:00' AND TIMESTAMP '2024-09-28 23:59:59'")
                user = cursor.fetchone()
                print(f"连接成功，当前用户: {user[0]}")  # 打印当前用户以验证连接

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
