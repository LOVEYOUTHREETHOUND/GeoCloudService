from waitress import serve
from src.geocloudservice.apis import gen_app
from src.geocloudservice.blueprints.recommend_query_bp import rz_app


def main():
    app = gen_app()
    app.run("0.0.0.0", 12345)


