import pytest
from cid_minter.create_sqlite_db import create_connection, execute_sql, insert_query, list_data

@pytest.fixture
#def create_test_db(datadir):
#    database = "{}/test_sqlite_1.db".format(datadir)
def create_test_db(tmpdir):
    database = tmpdir/"test_sqlite_1.db"
    sql_drop_table = "DROP TABLE IF EXISTS cid_minting_store"
    sql_create_table = """ CREATE TABLE IF NOT EXISTS cid_minting_store (
        id integer PRIMARY KEY AUTOINCREMENT,
        type char(50) DEFAULT NULL,
        identifier char(255) DEFAULT NULL,
        cid char(11) DEFAULT NULL
         ); """

    conn = create_connection(database)
    conn.execute(sql_drop_table)
    conn.execute(sql_create_table)
    insert_data(conn)

    return conn

def insert_data(conn):
    sql = "INSERT INTO cid_minting_store(type, identifier, cid) VALUES(?,?,?)" 
    minting_data = [("oclc", "8727632", "002492721"),
            ("icontrib_sys_id", "pur215476", "002492721"),
            ("oclc", "32882115", "011323405"),
            ("contrib_sys_id", "pur864352", "011323405")]

    for data in minting_data:
        insert_query(conn, sql, data)
    conn.commit()

def test_list_data(create_test_db):
    """ the 'create_test_db' argument here is matched to the name of the
        fixture above
    """

    select_sql = "select * from cid_minting_store"
    rows = list_data(create_test_db, select_sql)
    assert len(rows) == 4

