import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    #   print(sqlite3.version)
    except Error as e:
        print(e)
    return conn

def execute_sql(conn, sql):
    """ execut a sql statement
    :param conn: Connection object
    :param sql: a SQL statement
    :return:
    """
    try:
        cur = conn.cursor()
        cur.execute(sql)
    except Error as e:
        print(e)

def insert_query(conn, sql, data):
    """
    :param conn:
    :param data:
    :return:
    """
    try:
        cur = conn.cursor()
        cur.execute(sql, data)
        return cur.lastrowid
    except Error as e:
        print(e)
        return 0

def insert_data(conn):
    sql = "INSERT INTO cid_minting_store(type, identifier, cid) VALUES(?,?,?)" 
    minting_data = [("oclc", "8727632", "002492721"),
            ("icontrib_sys_id", "pur215476", "002492721"),
            ("oclc", "32882115", "011323405"),
            ("contrib_sys_id", "pur864352", "011323405")]

    for data in minting_data:
        insert_query(conn, sql, data)
    conn.commit()

def list_data(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    return rows

def main():
    database = r"database/test_sqlite.db"

    sql_drop_table = "DROP TABLE IF EXISTS cid_minting_store" 
    sql_create_table = """ CREATE TABLE IF NOT EXISTS cid_minting_store (
        id integer PRIMARY KEY AUTOINCREMENT,
        type char(50) DEFAULT NULL,
        identifier char(255) DEFAULT NULL,
        cid char(11) DEFAULT NULL
         )"""
    sql_select = "select * from cid_minting_store"

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        execute_sql(conn, sql_drop_table)
        execute_sql(conn, sql_create_table)
        insert_data(conn)
        rows = list_data(conn, sql_select)
        for row in rows:
            print(row)
    else:
        print("Error! cannot create the database connection.")

if __name__ == '__main__':
    main()
