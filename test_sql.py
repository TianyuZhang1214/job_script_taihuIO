
database = '_read'

sql = "select * from " + database+ " where JOBname='"+ +"' and CORE="


def select(jobname, jobcore, database):
    global conn
    conn=MySQLdb.connect(host='20.0.2.15',user='swqh',db='JOB',passwd='123456',port=3306)
    try:
        cursor=conn.cursor()
        cursor.execute(sql)
        result=cursor.fetchall()
        conn.commit()
        cursor.close()
    except Exception as e:
        print e
        conn.rollback()
    return result



