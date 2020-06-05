import MySQLdb

class Minter:
    def __init__(self, database):
        self._conn = MySQLdb.connect(database)
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
    pw = "htmmrw3dev"

    database_info = ("localhost","root","1234","users")
    minter = Minter(database_info)
    sql = "select * from cid_minting_store"
    minter.execute(sql)
    print(minter.fetchone)
    print(p1.age)

if __name__ == '__main__':
    main()
