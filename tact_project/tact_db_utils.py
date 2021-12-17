import os
import sys

from sqlalchemy import create_engine
from sqlalchemy import text

SELECT_TACT = "SELECT id, publisher, doi FROM transactions WHERE id=:id"

class Database:
    def __init__(self, db_connect_str):
        self.engine = create_engine(db_connect_str)

    def findall(self, sql, params=None):
        with self.engine.connect() as connection:
            results = connection.execute(sql, params or ())
            results_dict = [dict(row) for row in results.fetchall()]
            return results_dict

    def close(self):
        self.engine.dispose()

def find_records(db_connect_str, select_query, params=None):
    """
    Args:
        db_connect_str: database connection string
        sql_select_query: SQL select query
    Returns:
        list of dict with selected field names as keys
    """
    if select_query:
        try:
            db = Database(db_connect_str)
            records = db.findall(text(select_query), params)
            db.close()
            return records
        except:
            return None
    return None


def find_tact_transactions(db_connect_str, id):
    params = {"id": id}
    return find_records(db_connect_str, SELECT_TACT, params)


