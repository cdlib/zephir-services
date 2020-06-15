import os

import pytest
from create_sqlite_db import create_connection, execute_sql, insert_query, list_data
from mint_cid import find_all, find_one

@pytest.fixture
def create_test_db(request, data_dir, tmpdir):
    database = tmpdir/"test_sqlite.db"
    #database = os.path.join(data_dir, "test_sqlite.db")
    sql = os.path.join(data_dir, "setup_cid_minter_db.sql")
    
    os.system("sqlite3 {} < {}".format(database, sql))
    conn = create_connection(database)
    return conn


def test_list_data(create_test_db):
    """ the 'create_test_db' argument here is matched to the name of the
        fixture above
    """

    select_sql = "select * from cid_minting_store"
    rows = list_data(create_test_db, select_sql)
    assert len(rows) == 5

