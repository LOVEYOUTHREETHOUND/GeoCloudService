from waitress import serve
from src.geocloudservice.apis import gen_app
from src.utils.db.oracle import create_pool


def main():
    app = gen_app()
    app.run("0.0.0.0", 12345)

