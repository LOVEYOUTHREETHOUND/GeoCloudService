import src.geocloudservice.db.oracle as oracle

def test_pool():
    pool = oracle.create_pool()
    conn = pool.connection()
    cursor = conn.cursor()
    cursor.execute('select * from TF_ORDER')
    result = cursor.fetchall()
    print(result)
    cursor.close()
    conn.close()
    
    
def test_dbconn():
    conn = oracle.create_dbconn()
    cursor = conn.cursor()
    cursor.execute('select * from TF_ORDER')
    result = cursor.fetchall()
    print(result)
    cursor.close()
    conn.close()

test_dbconn()
