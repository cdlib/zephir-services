import os
import sys

import pytest
import environs
import logging

from local_cid_minter import prepare_database, find_all, find_by_identifier, find_query, insert_a_record, find_cids_by_ocns, find_cid_by_sysid, main

@pytest.fixture
def create_test_db(data_dir, tmpdir, scope="session"):
    db_name = "test_minter_sqlite.db"
    #database = os.path.join(tmpdir, db_name)
    database = os.path.join(data_dir, db_name)
    create_table = os.path.join(data_dir, "create_cid_minting_store_table.sql")
    insert_data = os.path.join(data_dir, "prepare_cid_minter_datasets.sql")
    
    cmd = "sqlite3 {} < {}".format(database, create_table)
    os.system(cmd)
    cmd = "sqlite3 {} < {}".format(database, insert_data)
    os.system(cmd)

    db_conn_str = 'sqlite:///{}'.format(database)
    os.environ["OVERRIDE_DB_CONNECT_STR"] = db_conn_str

    return {'db_conn_str': db_conn_str}

def test_find_query(create_test_db):
    """ the 'create_test_db' argument here is matched to the name of the
        fixture above
    """
    db_conn_str = create_test_db['db_conn_str']
    db = prepare_database(db_conn_str)
    engine = db["engine"]
    session = db["session"]
    CidMintingStore = db["table"]

    select_sql = "select type, identifier, cid from cid_minting_store"
    results = find_query(engine, select_sql)
    print(results)
    assert len(results) == 5

    # expected results:
    expected_results = [
            {'type': 'ocn', 'identifier': '8727632', 'cid': '002492721'}, 
            {'type': 'sysid', 'identifier': 'pur215476', 'cid': '002492721'}, 
            {'type': 'ocn', 'identifier': '32882115', 'cid': '011323405'}, 
            {'type': 'sysid', 'identifier': 'pur864352', 'cid': '011323405'}, 
            {'type': 'sysid', 'identifier': 'uc1234567', 'cid': '011323405'}]

    for record in results:
        assert any(expected_result == record for expected_result in expected_results)


def test_find_by_identifier(create_test_db):
    db_conn_str = create_test_db['db_conn_str']
    db = prepare_database(db_conn_str)
    engine = db["engine"]
    session = db["session"]
    CidMintingStore = db["table"]

    record = find_by_identifier(CidMintingStore, session, 'ocn', '8727632')
    print(record)
    assert [record.type, record.identifier, record.cid] == ['ocn', '8727632', '002492721']

    record = find_by_identifier(CidMintingStore, session, 'ocn', '1234567890')
    assert record == None 

    record = find_by_identifier(CidMintingStore, session, 'ocn', '')
    assert record == None

    record = find_by_identifier(CidMintingStore, session, 'sysid', 'pur215476')
    print(record)
    assert [record.type, record.identifier, record.cid] == ['sysid', 'pur215476', '002492721']

    record = find_by_identifier(CidMintingStore, session, 'sysid', 'xyz12345')
    assert record == None 

    record = find_by_identifier(CidMintingStore, session, 'sysid', '')
    assert record == None

def test_find_all(create_test_db):
    db_conn_str = create_test_db['db_conn_str']
    db = prepare_database(db_conn_str)
    engine = db["engine"]
    session = db["session"]
    CidMintingStore = db["table"]

    results = find_all(CidMintingStore, session)
    print(type(results))
    assert len(results) == 5
    assert any([record.type, record.identifier, record.cid] == ['ocn', '8727632', '002492721'] for record in results)
    assert any([record.type, record.identifier, record.cid] == ['sysid', 'pur864352', '011323405'] for record in results)

def test_insert_a_record(caplog, create_test_db):
    caplog.set_level(logging.DEBUG)

    db_conn_str = create_test_db['db_conn_str']
    db = prepare_database(db_conn_str)
    engine = db["engine"]
    session = db["session"]
    CidMintingStore = db["table"]

    # before insert a record
    results = find_all(CidMintingStore, session)
    assert len(results) == 5

    record = CidMintingStore(type='ocn', identifier='30461866', cid='011323406')
    insert_a_record(session, record)
    # after insert a record
    results = find_all(CidMintingStore, session)
    assert len(results) == 6
    assert any([record.type, record.identifier, record.cid] == ['ocn', '30461866', '011323406'] for record in results)
    
    # insert the same record
    record = CidMintingStore(type='ocn', identifier='30461866', cid='011323406')
    insert_a_record(session, record)
    assert "IntegrityError adding record" in caplog.text
    results = find_all(CidMintingStore, session)
    assert len(results) == 6

def test_find_cids_by_ocns(create_test_db):
    db_conn_str = create_test_db['db_conn_str']
    db = prepare_database(db_conn_str)
    engine = db["engine"]
    session = db["session"]
    CidMintingStore = db["table"]

    ocns_list = ['8727632', '32882115']
    expected_results = { 
        'inquiry_ocns': ['8727632', '32882115'],
        'matched_cids': [{'cid': '002492721'}, {'cid': '011323405'}],
        'min_cid': '002492721',
        'num_of_cids': 2,
    }

    results = find_cids_by_ocns(engine, ocns_list)
    print(results)

    assert results['min_cid'] == expected_results['min_cid']
    assert results['num_of_cids'] == expected_results['num_of_cids']
    
    for result in results['inquiry_ocns']:
        assert any(expected_result == result for expected_result in expected_results['inquiry_ocns'])
    for result in results['matched_cids']:
        assert any(expected_result == result for expected_result in expected_results['matched_cids'])

def test_find_cids_by_ocns_none(create_test_db):
    db_conn_str = create_test_db['db_conn_str']
    db = prepare_database(db_conn_str)
    engine = db["engine"]
    session = db["session"]
    CidMintingStore = db["table"]

    ocns_list = []
    expected = {
        'inquiry_ocns': [], 
        'matched_cids': [], 
        'min_cid': None, 
        'num_of_cids': 0
    }
    results = find_cids_by_ocns(engine, ocns_list)
    assert results ==expected

    ocns_list = ['1234567890']
    expected = {
        'inquiry_ocns': ['1234567890'], 
        'matched_cids': [], 
        'min_cid': None, 
        'num_of_cids': 0
    }
    results = find_cids_by_ocns(engine, ocns_list)
    assert results == expected

def test_find_cid_by_sysid(create_test_db):
    db_conn_str = create_test_db['db_conn_str']
    db = prepare_database(db_conn_str)
    engine = db["engine"]
    session = db["session"]
    CidMintingStore = db["table"]

    sysid = ""
    expected = {}
    result = find_cid_by_sysid(CidMintingStore, session, sysid)
    print(result)
    assert result == expected

    sysid = "xyz123"
    expected = {}
    result = find_cid_by_sysid(CidMintingStore, session, sysid)
    print(result)
    assert result == expected

    sysid = "uc1234567"
    expected = {
        'inquiry_sys_id': 'uc1234567',
        'matched_cid': '011323405'}
    result = find_cid_by_sysid(CidMintingStore, session, sysid)
    print(result)
    assert result == expected

# no argument
def test_main_param_err_0(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        main()
    out, err = capsys.readouterr()
    assert "Parameter error" in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 1]

# one argument
def test_main_param_err_1(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['']
        main()
    out, err = capsys.readouterr()
    assert "Parameter error" in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 1]

def test_main_param_err_2(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'dev']
        main()
    out, err = capsys.readouterr()
    assert "Parameter error" in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 1]

def test_main_read_by_ocns(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'test', 'read', 'ocn', '8727632,32882115']
        main()
    out, err = capsys.readouterr()
    expected = '{"inquiry_ocns": ["8727632", "32882115"], "matched_cids": [{"cid": "011323405"}, {"cid": "002492721"}], "min_cid": "002492721", "num_of_cids": 2}'
    assert expected in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]

def test_main_read_by_ocns_not_exists(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'test', 'read', 'ocn', '1234567890']
        main()
    out, err = capsys.readouterr()
    expected = '{"inquiry_ocns": ["1234567890"], "matched_cids": [], "min_cid": null, "num_of_cids": 0}'
    assert expected in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]

def test_main_read_by_sysid(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'test', 'read', 'sysid', 'pur215476']
        main()
    out, err = capsys.readouterr()
    expected = '{"inquiry_sys_id": "pur215476", "matched_cid": "002492721"}'
    assert expected in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]

def test_main_read_by_sysid_not_exists(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'test', 'read', 'sysid', 'sysid123']
        main()
    out, err = capsys.readouterr()
    expected = '{}'
    assert expected in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]

def test_main_write_ocn(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'test', 'write', 'ocn', '123456789', '100000000']
        main()
    out, err = capsys.readouterr()
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]

def test_main_write_sysid(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'test', 'write', 'sysid', 'XY1234567', '200000000']
        main()
    out, err = capsys.readouterr()
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]

# 3|ocn|32882115|011323405|2020-09-16 00:09:26
def test_main_write_ocn_dup(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'test', 'write', 'ocn', '32882115', '011323405']
        main()
    out, err = capsys.readouterr()
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 1]

# 4|sysid|pur864352|011323405|2020-09-16 00:09:26
def test_main_write_sysid_dup(capsys, create_test_db):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ['', 'test', 'write', 'sysid', 'pur864352', '011323405']
        main()
    out, err = capsys.readouterr()
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 1]

