from flask import Flask
import cx_Oracle

app = Flask(__name__)

@app.route('/get-order-count', methods=['GET'])
def get_order_count():
    # 数据库连接配置
    dsn = cx_Oracle.makedsn('你的数据库地址', '端口号', service_name='服务名')
    conn = cx_Oracle.connect('用户名', '密码', dsn)
    
    try:
        cursor = conn.cursor()
        # SQL查询语句
        sql = """
        SELECT COUNT(*) AS total
        FROM TF_ORDER
        WHERE orderStatusIndex NOT IN (-1, 0, 2)
        """
        cursor.execute(sql)
        result = cursor.fetchone()
        total_orders = result[0]
        return {'total_orders': total_orders}, 200
    except cx_Oracle.DatabaseError as e:
        return {'error': str(e)}, 500
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    app.run(debug=True)