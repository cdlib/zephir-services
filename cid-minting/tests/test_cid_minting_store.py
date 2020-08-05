import os

import pytest
import environs
from cid_minting_store import prepare_database, find_all, find_by_ocn, find_query, insert_a_record

@pytest.fixture
def create_test_db(data_dir, tmpdir, scope="session"):
    db_name = "test_minter_sqlite.db"
    #database = os.path.join(tmpdir, db_name)
    database = os.path.join(data_dir, db_name)
    setup_sql = os.path.join(data_dir, "setup_cid_minter_db.sql")
    
    cmd = "sqlite3 {} < {}".format(database, setup_sql)
    print(cmd)
    os.system(cmd)

    os.environ["OVERRIDE_DB_CONNECT_STR"] = 'sqlite:///{}'.format(database)
    print("set env: {}".format(os.environ.get("OVERRIDE_DB_CONNECT_STR")))

def test_find_query(create_test_db):
    """ the 'create_test_db' argument here is matched to the name of the
        fixture above
    """
    db_conn_str = os.environ.get("OVERRIDE_DB_CONNECT_STR")
    print("in test_find_query db_conn_str: {}".format(db_conn_str))
    select_sql = "select * from cid_minting_store"

    engine, session, CidMintingStore = prepare_database(db_conn_str)
    results = find_query(engine, select_sql)
    print(results)
    assert len(results) == 5

def test_find_by_ocn():
    db_conn_str = os.environ.get("OVERRIDE_DB_CONNECT_STR")
    engine, session, CidMintingStore = prepare_database(db_conn_str)
    record = find_by_ocn(CidMintingStore, session, '8727632')
    print(record)
    assert [record.type, record.identifier, record.cid] == ['oclc', '8727632', '002492721']

def test_find_all():
    db_conn_str = os.environ.get("OVERRIDE_DB_CONNECT_STR")
    engine, session, CidMintingStore = prepare_database(db_conn_str)
    results = find_all(CidMintingStore, session)
    print(type(results))
    assert len(results) == 5
    record = CidMintingStore(type='oclc', identifier='8727632', cid='002492721')
    print(record)
    assert any([record.type, record.identifier, record.cid] == ['oclc', '8727632', '002492721'] for record in results)
    assert any([record.type, record.identifier, record.cid] == ['contrib_sys_id', 'pur864352', '011323405'] for record in results)

def test_insert_a_record():
    db_conn_str = os.environ.get("OVERRIDE_DB_CONNECT_STR")
    engine, session, CidMintingStore = prepare_database(db_conn_str)
    # before insert a record
    results = find_all(CidMintingStore, session)
    assert len(results) == 5

    record = CidMintingStore(type='oclc', identifier='30461866', cid='011323406')
    insert_a_record('log', session, record)
    # after insert a record
    results = find_all(CidMintingStore, session)
    assert len(results) == 6

    assert any([record.type, record.identifier, record.cid] == ['oclc', '30461866', '011323406'] for record in results)
