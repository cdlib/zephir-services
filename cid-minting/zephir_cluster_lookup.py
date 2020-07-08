
from sqlalchemy import create_engine
from sqlalchemy import text

import mysql.connector as mysql 

class ZephirDatabase:
    def __init__(self, db_connect_str):
        self.engine = create_engine(db_connect_str)

    def query(self, sql, params=None):
        with self.engine.connect() as connection:
            results = connection.execute(sql, params or ())
            return results.fetchall()

def get_zephir_cluster_by_ocn(db_conn_str, sql):
    z = ZephirDatabase(db_conn_str)
    return z.query(text(sql))

def main():

    database = "htmm"
    host = "rds-d2d-htmm-dev.cmcguhglinoa.us-west-2.rds.amazonaws.com"
    user = "htmmrw"
    password = "htmmrw3dev"
    port = "3306"

    sql_a = """select distinct z.cid, i.identifier
    from zephir_records as z
    inner join zephir_identifier_records as r on r.record_autoid = z.autoid
    inner join zephir_identifiers as i on i.autoid = r.identifier_autoid
    where i.type = 'oclc' and i.identifier in 
    """
    sql_b = "order by z.cid, z.id"
    oclc_list = "'6758168','15437990','5663662','33393343','28477569','8727632'"
    sql = sql_a + " (" + oclc_list + ") " + sql_b

# use sqlalchemy 
    #db_conn_str = 'dialect+driver://user:pass@host:port/db'
    #db_conn_str = "sqlite:///database/test_zephir_sqlite.db"
    db_conn_str = "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(user, password, host, port, database)

    print("combined string results: ")
    results = get_zephir_cluster_by_ocn(db_conn_str, sql)
    print(results)


# use mysql.connector
    database_info = {'host': host, 'user': user, 'password': password, 'database': database}

    print("mysql connector")
    cluster = test_sql_connector(database_info, sql)
    print(cluster)

class ZephirDatabaseMysql:
    def __init__(self, database):
        self._conn = mysql.connect(**database)
        self._cursor = self._conn.cursor()

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def close(self, commit=True):
        if commit:
            self.commit()
        self.connection.close()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()

def test_sql_connector(database_info, sql):
    zephir = ZephirDatabaseMysql(database_info)
    zephir.execute(sql)
    return zephir.fetchall()

if __name__ == '__main__':
    main()
