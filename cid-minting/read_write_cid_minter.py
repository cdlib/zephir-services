import mysql.connector as mysql 

class Minter:
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

def main():

    database = "htmm"
    host = "rds-d2d-htmm-dev.cmcguhglinoa.us-west-2.rds.amazonaws.com"
    user = "htmmrw"
    password = "htmmrw3dev"

 #   database_info = dict(host=host, user=user, password=password, database=database)
    database_info = {'host': host, 'user': user, 'password': password, 'database': database}

    minter = Minter(database_info)
    sql = "select * from cid_minting_store"
    sql = """select distinct z.cid, i.identifier
    from zephir_records as z
    inner join zephir_identifier_records as r on r.record_autoid = z.autoid
    inner join zephir_identifiers as i on i.autoid = r.identifier_autoid
    where i.type = 'oclc' and i.identifier in (
    '6758168',
    '15437990',
    '5663662',
    '33393343',
    '28477569',
    '8727632')
    order by z.cid, z.id
    """

    minter.execute(sql)
    #m = minter.fetchone()
    #print(type(m))
    #print(m)

    m = minter.fetchall()
    print(type(m))
    print(m)

if __name__ == '__main__':
    main()
