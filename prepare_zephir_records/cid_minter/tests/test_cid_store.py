import os
import sys

import pytest
import environs
import logging

from cid_minter.cid_store import CidStore

@pytest.fixture
def create_test_db(data_dir, tmpdir, scope="session"):
    db_name = "test_minter_sqlite.db"
    #database = os.path.join(tmpdir, db_name)
    database = os.path.join(data_dir, db_name)
    prepare_datasets = os.path.join(data_dir, "prepare_cid_minter_datasets.sql")
    
    cmd = "sqlite3 {} < {}".format(database, prepare_datasets)
    os.system(cmd)

    db_conn_str = 'sqlite:///{}'.format(database)
    os.environ["OVERRIDE_DB_CONNECT_STR"] = db_conn_str

    return {'db_conn_str': db_conn_str}

"""
    datasets in test DB
            {'type': 'ocn', 'identifier': '8727632', 'cid': '002492721'}, 
            {'type': 'sysid', 'identifier': 'pur215476', 'cid': '002492721'}, 
            {'type': 'ocn', 'identifier': '32882115', 'cid': '011323405'}, 
            {'type': 'sysid', 'identifier': 'pur864352', 'cid': '011323405'}, 
            {'type': 'sysid', 'identifier': 'uc1234567', 'cid': '011323405'}
"""

def test_find_record_by_identifier(create_test_db):
    db_conn_str = create_test_db['db_conn_str']
    db = CidStore(db_conn_str)
    engine = db.engine
    session = db.session
    CidMintingStore = db.tablename

    record = db._find_record_by_identifier('ocn', '8727632')
    print(record)
    assert [record.type, record.identifier, record.cid] == ['ocn', '8727632', '002492721']

    record = db._find_record_by_identifier('ocn', '1234567890')
    assert record == None 

    record = db._find_record_by_identifier('ocn', '')
    assert record == None

    record = db._find_record_by_identifier('sysid', 'pur215476')
    print(record)
    assert [record.type, record.identifier, record.cid] == ['sysid', 'pur215476', '002492721']

    record = db._find_record_by_identifier('sysid', 'xyz12345')
    assert record == None 

    record = db._find_record_by_identifier('sysid', '')
    assert record == None

def test_find_record(create_test_db):
    db_conn_str = create_test_db['db_conn_str']
    db = CidStore(db_conn_str)
    engine = db.engine
    session = db.session
    CidMintingStore = db.tablename

    ocn = "8727632"
    cid = "002492721"
    record = CidMintingStore(type="ocn", identifier=ocn, cid=cid)

    ret = db._find_record(record)
    assert [ret.type, ret.identifier, ret.cid] == [record.type, record.identifier, record.cid]

    ocn = "1234567890"
    cid = "9912345678"
    record = CidMintingStore(type="ocn", identifier=ocn, cid=cid)

    ret = db._find_record(record)
    assert ret == None

def test_find_all(create_test_db):
    db_conn_str = create_test_db['db_conn_str']
    db = CidStore(db_conn_str)
    engine = db.engine
    session = db.session
    CidMintingStore = db.tablename

    results = db._find_all()
    print(type(results))
    assert len(results) == 5
    assert any([record.type, record.identifier, record.cid] == ['ocn', '8727632', '002492721'] for record in results)
    assert any([record.type, record.identifier, record.cid] == ['sysid', 'pur864352', '011323405'] for record in results)

def test_find_cid_by_ocn(create_test_db):
    db_conn_str = create_test_db['db_conn_str']
    db = CidStore(db_conn_str)
    engine = db.engine
    session = db.session
    CidMintingStore = db.tablename

    ocn = ""
    expected = {}
    result = db.find_cid("ocn", ocn)
    print(result)
    assert result == expected

    ocn = "1234567890"
    expected = {}
    result = db.find_cid("ocn", ocn)
    print(result)
    assert result == expected

    ocn = "8727632"
    expected = {
        'data_type': "ocn",
        'inquiry_identifier': "8727632",
        'matched_cid': "002492721"}
    result = db.find_cid("ocn", ocn)
    print(result)
    assert result == expected

def test_find_cid_by_sysid(create_test_db):
    db_conn_str = create_test_db['db_conn_str']
    db = CidStore(db_conn_str)
    engine = db.engine
    session = db.session
    CidMintingStore = db.tablename

    sysid = ""
    expected = {}
    result = db.find_cid("sysid", sysid)
    print(result)
    assert result == expected

    sysid = "xyz123"
    expected = {}
    result = db.find_cid("sysid", sysid)
    print(result)
    assert result == expected

    sysid = "uc1234567"
    expected = {
        'data_type': "sysid",
        'inquiry_identifier': 'uc1234567',
        'matched_cid': '011323405'}
    result = db.find_cid("sysid", sysid)
    print(result)
    assert result == expected

def test_update_an_existing_record(caplog, create_test_db):
    caplog.set_level(logging.DEBUG)

    db_conn_str = create_test_db['db_conn_str']
    db = CidStore(db_conn_str)
    engine = db.engine
    session = db.session
    CidMintingStore = db.tablename

    # existing record
    ocn = "8727632"
    cid = "002492721"
    expected = {
        'data_type': "ocn",
        'inquiry_identifier': ocn,
        'matched_cid': cid}
    result = db.find_cid("ocn", ocn)
    print(result)
    assert result == expected

    # update cid for this record
    cid = "123456789"
    updated_rd = CidMintingStore(type="ocn", identifier=ocn, cid=cid)
    ret = db._update_a_record(updated_rd)
    assert ret == 1

    expected = {
        'data_type': "ocn",
        'inquiry_identifier': ocn,
        'matched_cid': cid}
    result = db.find_cid("ocn", ocn)
    print(result)
    assert result == expected

def test_update_a_non_existing_record(caplog, create_test_db):
    caplog.set_level(logging.DEBUG)

    db_conn_str = create_test_db['db_conn_str']
    db = CidStore(db_conn_str)
    engine = db.engine
    session = db.session
    CidMintingStore = db.tablename

    # non-existing record
    ocn = "1234567890123"
    cid = "9999123456789"
    result = db.find_cid("ocn", ocn)
    print(result)
    assert result == {} 

    # updated record
    cid = "123456789"
    updated_rd = CidMintingStore(type="ocn", identifier=ocn, cid=cid)
    ret = db._update_a_record(updated_rd)
    assert ret == 0

    expected = {}
    result = db.find_cid("ocn", ocn)
    print(result)
    assert result == expected

def test_insert_a_record(caplog, create_test_db):
    caplog.set_level(logging.DEBUG)

    db_conn_str = create_test_db['db_conn_str']
    db = CidStore(db_conn_str)
    engine = db.engine
    session = db.session
    CidMintingStore = db.tablename

    # before insert a record
    results = db._find_all()
    assert len(results) == 5

    record = CidMintingStore(type='ocn', identifier='30461866', cid='011323406')
    ret = db._insert_a_record(record)
    assert ret == 1   # success insert
    # after insert a record
    results = db._find_all()
    assert len(results) == 6
    assert any([record.type, record.identifier, record.cid] == ['ocn', '30461866', '011323406'] for record in results)

    # insert the same record
    record = CidMintingStore(type='ocn', identifier='30461866', cid='011323406')
    ret = db._insert_a_record(record)
    assert ret is None    # failed insert
    assert "database error" in caplog.text.lower()

    results = db._find_all()
    assert len(results) == 6
    assert any([record.type, record.identifier, record.cid] == ['ocn', '30461866', '011323406'] for record in results)

# Note: the test database saves all changes from the last test function
def test_write_a_record(create_test_db):
    db_conn_str = create_test_db['db_conn_str']
    db = CidStore(db_conn_str)
    engine = db.engine
    session = db.session
    CidMintingStore = db.tablename

    # existing record
    ocn = "8727632"
    cid = "002492721"
    expected = {
        'data_type': "ocn",
        'inquiry_identifier': ocn,
        'matched_cid': cid}
    result = db.find_cid("ocn", ocn)
    print(result)
    assert result == expected

    ret = db.write_identifier("ocn", ocn, cid)
    assert ret == "Local minter: Record exists. No need to update"

    # update cid for this record
    ocn = "8727632"
    cid = "123456789"
    expected = {
        'data_type': "ocn",
        'inquiry_identifier': ocn,
        'matched_cid': cid}
    ret = db.write_identifier("ocn", ocn, cid)
    assert ret == "Local minter: Updated an exsiting record"

    result = db.find_cid("ocn", ocn)
    assert result == expected

    # insert a new record
    ocn = "1234567890123"
    cid = "9999123456789"
    result = db.find_cid("ocn", ocn)
    assert result == {}

    expected = {
        'data_type': "ocn",
        'inquiry_identifier': ocn,
        'matched_cid': cid}
    ret = db.write_identifier("ocn", ocn, cid)
    assert ret == "Local minter: Inserted a new record"

    result = db.find_cid("ocn", ocn)
    assert result == expected


