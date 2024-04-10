import pymysql


def get_database():

    return pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='password',
        db='timezone_db',
        charset='utf8mb4',
        connect_timeout=30,
        cursorclass=pymysql.cursors.DictCursor
    )
