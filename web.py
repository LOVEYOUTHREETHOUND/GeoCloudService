from waitress import serve
from src.geocloudservice.apis import gen_app


# def main():
#     app = gen_app()
#     app.run("0.0.0.0", 12345)
    # serve(app,  port=12345)


if __name__ == "__main__":
    serve(gen_app(), host='0.0.0.0', port=5000)


