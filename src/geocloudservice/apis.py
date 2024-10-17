from flask import Flask, request, jsonify, Blueprint
from flask_siwadoc import SiwaDoc
from waitress import serve

from src.geocloudservice.blueprints.spatial_query_bp import spatial_query_blueprint
from src.geocloudservice.api_models import TimespanQueryModel
from src.utils.db.oracle import create_pool
import src.config.config as config


def gen_app():
    app = Flask(__name__, )
    siwa = SiwaDoc(app, title="FJY API", description="地质云航遥节点遥感数据服务系统接口文档")
    pool = create_pool()

    spatial_query_bp = spatial_query_blueprint(siwa, pool)
    app.register_blueprint(spatial_query_bp)

    @app.post(f"/test")
    @siwa.doc(
        summary="测试接口",
        description="测试接口",
        tags=["test"],
    )
    def test():
        return jsonify({"code": 200, "msg": "success"})
    bp_stats(app, siwa)
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
        return jsonify({"code": 200, "msg": "success"})

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
    


