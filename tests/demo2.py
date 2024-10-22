from flask import Flask, jsonify
import cx_Oracle

app = Flask(__name__)

@app.route("/", methods=['GET'])
def hello_world():
    return "Hello, World from Flask!"   



if __name__ == '__main__':
    # app.run(host='0.0.0.0', port=9999)
    app.run(debug=True)