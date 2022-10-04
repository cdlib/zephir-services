import os
import sys

import msgpack
import pytest
import plyvel
import json
import logging

from cid_minter.cid_minter import CidMinter

from cid_minter.zephir_cluster_lookup import ZephirDatabase
from cid_minter.local_cid_minter import LocalMinter
from cid_minter.oclc_lookup import get_ocns_cluster_by_ocn

from cid_minter.cid_inquiry_by_ocns import cid_inquiry_by_ocns
from cid_minter.cid_inquiry_by_ocns import flat_and_dedup_sort_list
from cid_minter.cid_inquiry_by_ocns import convert_comma_separated_str_to_int_list

"""Test the CidMinter class
   CID_Minter_Algorithm_PythonVersion_Spec:
   https://docs.google.com/document/d/1lkqjcN1axw8332kbby4dBg44gdYh5mnXMtlQ5a7_llw/edit#heading=h.gjdgxs
"""

def test_missing_htid_1(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    cid_minter = CidMinter(config)

    test_ids = {
        "no_htid": {"ocn": "123456789"},
        "empty_htid_1": {"htid": "", "ocn": "123456789"},
        "empty_htid_2": {"htid": " ", "ocn": "123456789"},
        "empty_htid_2": {"htid": "  ", "ocn": "123456789"},
    }

    for ids in test_ids:
        with pytest.raises(Exception) as e_info:
            cid = cid_minter.mint_cid(ids)
            assert "ValueError: ID error: missing required htid" in e_info

def test_step_0_1(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Step 0: find record's current CID if exists 
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    cid_minter = CidMinter(config)

    input_ids= {"htid": "hvd.hw5jdo"}

    # veriy in the Zephir DB
    FIND_CID_BY_ID = "SELECT cid FROM zephir_records WHERE id = 'hvd.hw5jdo'" 
    expected_cid = "009705704"
    results = zephirDb._get_query_results(FIND_CID_BY_ID)
    assert len(results) == 1
    assert results[0]['cid'] == expected_cid

    # verify class output
    cid = cid_minter.mint_cid(input_ids)
    assert "Found current CID: 009705704 by htid: hvd.hw5jdo" in caplog.text

def test_step_0_2(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Step 0: find record's current CID if exists 
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    cid_minter = CidMinter(config)

    input_ids= {"htid": "test.1234567890_0b"}

    # verify in the Zephir DB
    FIND_CID_BY_ID = "SELECT cid FROM zephir_records WHERE id = 'test.1234567890_0b'"
    results = zephirDb._get_query_results(FIND_CID_BY_ID)
    assert len(results) == 0 

    # verify class output
    cid = cid_minter.mint_cid(input_ids)
    assert "No CID/item found in Zephir DB by htid" in caplog.text

def test_step_1_a_1(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 1a: Found matched CID by OCNs in Local Minter.
       Use this CID and do not perform Zephir search.
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    cid_minter = CidMinter(config)
    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    local_minter = LocalMinter(setup_local_minter["local_minter"])
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]

    input_ids= {"ocns": "8727632", "htid": "test.1234567890_1b1"}

    # verify local minter: the following should be in the DB
    ocn = "8727632"
    cid = "002492721"
    record = local_minter._find_record_by_identifier("ocn", ocn)
    assert [record.type, record.identifier, record.cid] == ["ocn", ocn, cid]

    # test the CidMinter class
    cid = cid_minter.mint_cid(input_ids)
    assert cid == record.cid

    assert "Local minter: Found matched CID" in caplog.text
    assert "Find CID in Zephir Database by OCNs" not in caplog.text

def test_step_1_a_2(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 1a2: Found more than one matched CID by OCNs in Local Minter.
       Do not use this CID. Search CID in Zephir DB by OCNs in this case.
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    cid_minter = CidMinter(config)
    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    local_minter = LocalMinter(setup_local_minter["local_minter"])
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]

    input_ids= {"ocns": "8727632,87276322", "htid": "test.1234567890_1b1"}

    # verify local minter: the following should be in the DB
    ocn = "8727632"
    cid = "002492721"
    record = local_minter._find_record_by_identifier("ocn", ocn)
    assert [record.type, record.identifier, record.cid] == ["ocn", ocn, cid]

    ocn = "87276322"
    cid = "002492722"
    record = local_minter._find_record_by_identifier("ocn", ocn)
    assert [record.type, record.identifier, record.cid] == ["ocn", ocn, cid]

    # test the CidMinter class
    assigned_cid = cid_minter.mint_cid(input_ids)

    assert "Local minter error: Found more than one matched CID" in caplog.text
    assert "Find CID in Zephir Database by OCNs" in caplog.text

def test_step_1_a_3(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 1a3: No matched CID by OCNs in Local Minter.
       Search CID OCNs in Zephir by OCNs in this case.
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    cid_minter = CidMinter(config)
    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    local_minter = LocalMinter(setup_local_minter["local_minter"])
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]

    input_ids= {"ocns": "1234567890", "htid": "test.1234567890_1b1"}

    # verify local minter: the following should be in the DB
    ocn = "1234567890"
    record = local_minter._find_record_by_identifier("ocn", ocn)
    assert record is None

    # test the CidMinter class
    cid = cid_minter.mint_cid(input_ids)

    assert "Local minter: No CID found by OCN" in caplog.text
    assert "Find CID in Zephir Database by OCNs" in caplog.text

def test_step_1_b_1(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 1b1: Found matched cluster by OCNs in Zephir DB.
       Also verifies workflow and error conditions:
         - found current CID
         - reported warning OCLC Concordance Table does not contain record OCNs
         - found CID by OCNs
         - updated local minter
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"], 
    }

    cid_minter = CidMinter(config)
    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    local_minter = LocalMinter(setup_local_minter["local_minter"])
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]

    input_ids= {"ocns": "80274381,25231018", "contribsys_ids": "hvd000012735", "previous_sysids": "", "htid": "hvd.hw5jdo"}

    SELECT_ZEPHIR_BY_OCLC = """SELECT distinct z.cid cid, i.identifier ocn
        FROM zephir_records as z
        INNER JOIN zephir_identifier_records as r on r.record_autoid = z.autoid
        INNER JOIN zephir_identifiers as i on i.autoid = r.identifier_autoid
        WHERE z.cid != '0' AND i.type = 'oclc'
        AND i.identifier in ('80274381', '25231018')
    """
    # verify CID and OCNs in Zephir DB: ocn/cid are in zephir
    expected = [{"cid": "009705704", "ocn": "80274381"}, {"cid": "009705704", "ocn": "25231018"}]
    results = zephirDb._get_query_results(SELECT_ZEPHIR_BY_OCLC)
    for result in results:
        assert result in expected

    # verify in local minter: OCN not in local minter
    for ocn in ["80274381", "25231018"]:
        record = local_minter._find_record_by_identifier("ocn", ocn)
        assert record is None

    sysid = input_ids.get("contribsys_ids")
    record = local_minter._find_record_by_identifier("sysid", sysid)
    assert record is None

    # verify levelDB
    for ocn in [80274381, 25231018]:
        result = get_ocns_cluster_by_ocn(ocn, primary_db_path, cluster_db_path)
        assert result is None

    # test the CidMinter class
    expected_cid = "009705704"
    cid = cid_minter.mint_cid(input_ids)
    assert cid == expected_cid

    assert "Found current CID" in caplog.text
    assert "Local minter: No CID found by OCN" in caplog.text
    assert "OCLC Concordance Table does not contain record OCNs" in caplog.text
    assert "Zephir minter: Found matched CID" in caplog.text
    assert "Local minter: Inserted a new record" in caplog.text
    assert "Updated local minter: ocn: 80274381" in caplog.text
    assert "Updated local minter: ocn: 25231018" in caplog.text
    assert "Updated local minter: contribsys id: hvd000012735" in caplog.text

    # verify local minter has been updated
    for ocn in ["80274381", "25231018"]:
        record = local_minter._find_record_by_identifier("ocn", ocn)
        assert [record.type, record.identifier, record.cid] == ["ocn", ocn, expected_cid]

    record = local_minter._find_record_by_identifier("sysid", sysid)
    assert [record.type, record.identifier, record.cid] == ["sysid", sysid, expected_cid]

def test_step_1_b_2(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 1b2: Found matched cluster by OCNs in Zephir DB.
       Also verifies workflow and error conditions:
         - No CID/item found in Zephir DB by htid
         - Local minter: No CID found by OCN
         - report warning when OCNs matched to more than one primary OCLC number 
         - Zephir minter: No CID found by OCNs
         - Minted a new CID 
         - updated Local minter
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    cid_minter = CidMinter(config)
    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    local_minter = LocalMinter(setup_local_minter["local_minter"])
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]

    input_ids= {"ocns": "100,300", "htid": "test.1234567890_1a2"}

    SELECT_ZEPHIR_BY_OCLC = """SELECT distinct z.cid cid, i.identifier ocn
        FROM zephir_records as z
        INNER JOIN zephir_identifier_records as r on r.record_autoid = z.autoid
        INNER JOIN zephir_identifiers as i on i.autoid = r.identifier_autoid
        WHERE z.cid != '0' AND i.type = 'oclc'
        AND i.identifier in ('100', '300')
    """
    # verify CID and OCNs in Zephir DB: ocn/cid not in zephir
    results = zephirDb._get_query_results(SELECT_ZEPHIR_BY_OCLC)
    assert len(results) == 0 

    SELECT_MINTER = "SELECT cid from cid_minter"
    results = zephirDb._get_query_results(SELECT_MINTER)
    assert len(results) == 1
    minter = results[0].get("cid")

    # verify in local minter: OCN not in local minter
    for ocn in ["100", "300"]:
        record = local_minter._find_record_by_identifier("ocn", ocn)
        assert record is None

    # verify OCLC clusters in levelDB
    incoming_ocns = [100, 300]
    result = cid_inquiry_by_ocns(incoming_ocns, zephirDb, primary_db_path, cluster_db_path)
    assert result["num_of_matched_oclc_clusters"] == 2

    # test the CidMinter class
    cid = cid_minter.mint_cid(input_ids)
    # minted a new cid
    assert int(cid) == int(minter) + 1

    assert "No CID/item found in Zephir DB by htid" in caplog.text
    assert "match more than one OCLC Concordance clusters" in caplog.text
    assert "Local minter: No CID found by OCN" in caplog.text
    assert "Minted a new minter" in caplog.text
    assert "Local minter: Inserted a new record" in caplog.text
    assert "Updated local minter: ocn" in caplog.text

    # verify local minter has been updated
    for ocn in ["100", "300"]:
        record = local_minter._find_record_by_identifier("ocn", ocn)
        assert [record.type, record.identifier, record.cid] == ["ocn", ocn, cid]

    # verify cid_minter has been updated
    results = zephirDb._get_query_results(SELECT_MINTER)
    assert len(results) == 1
    minter_new = results[0].get("cid")
    assert int(minter_new) == int(minter) + 1

def test_step_1_b_3(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 1b3: Found matched cluster by OCNs in Zephir DB.
       Also verifies workflow and error conditions:
         - found current CID
         - matched more than one clusters, choose the lower one
         - found CID by OCNs
         - updated local minter
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    cid_minter = CidMinter(config)
    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    local_minter = LocalMinter(setup_local_minter["local_minter"])
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]

    input_ids= {"ocns": "80274381,25231018,30461866", "htid": "hvd.hw5jdo"}

    SELECT_ZEPHIR_BY_OCLC = """SELECT distinct z.cid cid, i.identifier ocn
        FROM zephir_records as z
        INNER JOIN zephir_identifier_records as r on r.record_autoid = z.autoid
        INNER JOIN zephir_identifiers as i on i.autoid = r.identifier_autoid
        WHERE z.cid != '0' AND i.type = 'oclc'
        AND i.identifier in ('80274381', '25231018', '30461866')
    """

    # verify CID and OCNs in Zephir DB: ocn/cid are in zephir
    expected = [{"cid": "009705704", "ocn": "80274381"}, 
                {"cid": "009705704", "ocn": "25231018"},
                {"cid": "011323406", "ocn": "30461866"}]
    results = zephirDb._get_query_results(SELECT_ZEPHIR_BY_OCLC)
    for result in results:
        assert result in expected

    # verify in local minter: OCN not in local minter
    for ocn in ["80274381", "25231018", "30461866"]:
        record = local_minter._find_record_by_identifier("ocn", ocn)
        assert record is None

    # test the CidMinter class
    expected_cid = min(int("009705704"), int("011323406"))
    cid = cid_minter.mint_cid(input_ids)
    assert int(cid) == expected_cid

    assert "matches 2 CIDs (['009705704', '011323406']) used 009705704" in caplog.text
    assert "Zephir minter: Found matched CID" in caplog.text


def test_step_2_a_1(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 2a1: Found one matched CID by contribsys ID in local minter.
       Use this CID and do not perform Zephir DB search in this case.
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    cid_minter = CidMinter(config)
    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    local_minter = LocalMinter(setup_local_minter["local_minter"])
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]

    input_ids= {"contribsys_ids": "pur215476", "htid": "test.2a1"}

    # sysid|pur215476|002492721
    sysid = input_ids.get("contribsys_ids")
    expected_cid = "002492721"

    # verify in local minter: sysid/cid are in local minter
    record = local_minter._find_record_by_identifier("sysid", sysid)
    assert [record.type, record.identifier, record.cid] == ["sysid", sysid, expected_cid] 

    # test the CidMinter class
    cid = cid_minter.mint_cid(input_ids)
    assert cid == expected_cid

    assert "Find CID in local minter by SYSID" in caplog.text
    assert f"Local minter: Found matched CID: ['{expected_cid}'] by SYSID: ['{sysid}']" in caplog.text
    assert "Find CID in Zephir Database by OCNs" not in caplog.text
    assert "Local minter: Record exists. No need to update" in caplog.text

    # verify local minter: sysid/id are not changed
    record = local_minter._find_record_by_identifier("sysid", sysid)
    assert [record.type, record.identifier, record.cid] == ["sysid", sysid, expected_cid]

def test_step_2_a_2(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 2a2: Found more than one matched CID by contribsys ID in local minter.
       Do not use this CID and search CID in Zephir DB in this case.
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    cid_minter = CidMinter(config)
    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    local_minter = LocalMinter(setup_local_minter["local_minter"])
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]

    input_ids= {"contribsys_ids": "pur215476,pur1234567", "htid": "test.2a1"}
    sysids = input_ids.get("contribsys_ids")
    cids = ["002492721", "002492727"]

    # sysid|pur215476|002492721
    sysid = "pur215476"
    expected_cid = "002492721"

    # verify in local minter: sysid/cid are in local minter
    record = local_minter._find_record_by_identifier("sysid", sysid)
    assert [record.type, record.identifier, record.cid] == ["sysid", sysid, expected_cid]

    sysid = "pur1234567"
    expected_cid = "002492727"

    record = local_minter._find_record_by_identifier("sysid", sysid)
    assert [record.type, record.identifier, record.cid] == ["sysid", sysid, expected_cid]

    # test the CidMinter class
    cid = cid_minter.mint_cid(input_ids)

    assert "Find CID in local minter by SYSID" in caplog.text
    assert f"Local minter error: Found more than one matched CID: {cids} by SYSID" in caplog.text
    assert "Find CID in Zephir Database by SYSID" in caplog.text


def test_step_2_a_3(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 2a3: No matched CID by contribsys ID in local minter.
       Search CID in Zephir DB by contribsys ID in this case.
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    cid_minter = CidMinter(config)
    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    local_minter = LocalMinter(setup_local_minter["local_minter"])
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]

    input_ids= {"contribsys_ids": "pur215476", "htid": "test.2a1"}

    # sysid|pur215476|002492721
    sysid = input_ids.get("contribsys_ids")
    expected_cid = "002492721"

    # verify in local minter: sysid/cid are in local minter
    record = local_minter._find_record_by_identifier("sysid", sysid)
    assert [record.type, record.identifier, record.cid] == ["sysid", sysid, expected_cid]

    # test the CidMinter class
    cid = cid_minter.mint_cid(input_ids)
    assert cid == expected_cid

    assert f"Local minter: Found matched CID: ['{expected_cid}'] by SYSID: ['{sysid}']" in caplog.text
    assert "Find CID in Zephir Database by OCNs" not in caplog.text
    assert "Local minter: Record exists. No need to update" in caplog.text

    # verify local minter: sysid/id are not changed
    record = local_minter._find_record_by_identifier("sysid", sysid)
    assert [record.type, record.identifier, record.cid] == ["sysid", sysid, expected_cid]

def test_step_2_b_1(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 2b1: Found one matched CID by contribsys ID in Zephir.
       Also verifies workflow and error conditions:
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    cid_minter = CidMinter(config)
    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    local_minter = LocalMinter(setup_local_minter["local_minter"])
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]

    input_ids= {"contribsys_ids": "hvd000012735", "htid": "hvd.hw5jdo"}

    sysid = input_ids.get("contribsys_ids")
    SELECT_ZEPHIR_BY_SYSID = f"SELECT distinct cid, contribsys_id from zephir_records WHERE contribsys_id = '{sysid}'"

    # verify CID and sysids in Zephir DB: sysid/cid are in zephir
    expected = [{"cid": "009705704", "contribsys_id": "hvd000012735"}]
    results = zephirDb._get_query_results(SELECT_ZEPHIR_BY_SYSID)
    for result in results:
        assert result in expected

    # verify in local minter: sysid not in local minter
    record = local_minter._find_record_by_identifier("sysid", sysid)
    assert record is None

    # test the CidMinter class
    expected_cid = "009705704"
    cid = cid_minter.mint_cid(input_ids)
    assert cid == expected_cid

    assert "Local minter: No CID found by SYSID" in caplog.text
    assert f"Zephir minter: Found matched CIDs: ['{cid}'] by contribsys IDs: ['{sysid}']" in caplog.text

def test_step_2_b_2(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 2b2: Found more than one matched CID by contribsys IDs in Zephir.
         - Report warning matched more than one CID 
         - Choose the lower CID to use
         - record changed CID
         - local minter was updated
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    cid_minter = CidMinter(config)
    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    local_minter = LocalMinter(setup_local_minter["local_minter"])
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]

    input_ids= {"contribsys_ids": "hvd000012735,nrlf.b100608668", "htid": "hvd.hw5jdo"}

    htid  = input_ids.get("htid")
    FIND_CID_BY_ID = f"SELECT cid FROM zephir_records WHERE id = '{htid}'"
    current_cid = "009705704"
    results = zephirDb._get_query_results(FIND_CID_BY_ID)
    assert len(results) == 1
    assert results[0]['cid'] == current_cid

    SELECT_ZEPHIR_BY_SYSID = """SELECT distinct cid, contribsys_id from zephir_records 
        WHERE contribsys_id in ('hvd000012735', 'nrlf.b100608668')
    """

    # verify CID and sysids in Zephir DB: sysid/cid are in zephir
    expected = [{"cid": "009705704", "contribsys_id": "hvd000012735"}, 
                {"cid": "000641789", "contribsys_id": "nrlf.b100608668"}]
    results = zephirDb._get_query_results(SELECT_ZEPHIR_BY_SYSID)
    assert len(results) == 2
    for result in results:
        assert result in expected

    # verify in local minter: sysid not in local minter
    for sysid in ["hvd000012735", "nrlf.b100608668"]:
        record = local_minter._find_record_by_identifier("sysid", sysid)
        assert record is None

    # test the CidMinter class
    expected_cid = min("000641789","009705704")  # the lower one
    cid = cid_minter.mint_cid(input_ids)
    assert cid == expected_cid

    assert f"Found current CID: {current_cid} by htid: {htid}" in caplog.text
    assert "Local minter: No CID found by SYSID" in caplog.text
    assert "Zephir minter: Found matched CIDs: ['000641789', '009705704'] by contribsys IDs: ['hvd000012735', 'nrlf.b100608668']" in caplog.text
    assert "Record with local number matches more than one CID" in caplog.text
    assert f"htid hvd.hw5jdo changed CID from: {current_cid} to: {expected_cid}" in caplog.text
    assert "Local minter: Inserted a new record" in caplog.text
    assert "Updated local minter: contribsys id" in caplog.text

    # verify local minter has been updated
    for sysid in ["hvd000012735", "nrlf.b100608668"]:
        record = local_minter._find_record_by_identifier("sysid", sysid)
        assert [record.type, record.identifier, record.cid] == ["sysid", sysid, expected_cid]

def test_step_3_a_1(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 3a1: Found one matched CID by previous contribsys ID in local minter.
       Use this CID and do not perform Zephir DB search.
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    cid_minter = CidMinter(config)
    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    local_minter = LocalMinter(setup_local_minter["local_minter"])
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]

    input_ids= {"contribsys_ids": "test.12345", "previous_contribsys_ids": "pur215476", "htid": "hvd.hw5jdo"}

    # sysid|pur215476|002492721
    sysid = input_ids.get("contribsys_ids")
    previous_sysid = input_ids.get("previous_contribsys_ids")
    expected_cid = "002492721"

    # verify in local minter: sysid/cid are not in local minter
    record = local_minter._find_record_by_identifier("sysid", sysid)
    assert record is None

    # verify in local minter: previous sysid/cid are in local minter
    record = local_minter._find_record_by_identifier("sysid", previous_sysid)
    assert [record.type, record.identifier, record.cid] == ["sysid", previous_sysid, expected_cid]

    # test the CidMinter class
    cid = cid_minter.mint_cid(input_ids)
    assert cid == expected_cid

    assert f"Local minter: Found matched CID: ['{expected_cid}'] by PREV_SYSID: ['{previous_sysid}']" in caplog.text
    assert "Find CID in Zephir Database by OCNs" not in caplog.text
    assert "Local minter: Record exists. No need to update" in caplog.text
    assert f"Updated local minter: contribsys id: {sysid}" in caplog.text
    assert f"Updated local minter: previous contribsys id: {previous_sysid}" in caplog.text

    # verify in local minter: previous sysid/cid are in local minter now
    record = local_minter._find_record_by_identifier("sysid", sysid)
    assert [record.type, record.identifier, record.cid] == ["sysid", sysid, expected_cid]

    record = local_minter._find_record_by_identifier("sysid", previous_sysid)
    assert [record.type, record.identifier, record.cid] == ["sysid", previous_sysid, expected_cid]

def test_step_3_a_2(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 3a2: Found more than one matched CID by previous contribsys IDs in local minter.
       Do not use this CID and search CID in Zephir DB by previous contribsys IDs.
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    cid_minter = CidMinter(config)
    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    local_minter = LocalMinter(setup_local_minter["local_minter"])
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]

    input_ids= {"contribsys_ids": "test.12345", "previous_contribsys_ids": "pur215476,pur.215476", "htid": "hvd.hw5jdo"}

    # sysid|pur215476|002492721
    sysid = input_ids.get("contribsys_ids")

    # verify in local minter: sysid/cid are not in local minter
    record = local_minter._find_record_by_identifier("sysid", sysid)
    assert record is None

    # verify in local minter: previous sysid/cid are in local minter
    expected_cids = ["002492721", "102492721"]
    previous_sysid = "pur215476"
    expected_cid = "002492721"
    record = local_minter._find_record_by_identifier("sysid", previous_sysid)
    assert [record.type, record.identifier, record.cid] == ["sysid", previous_sysid, expected_cid]

    previous_sysid = "pur.215476"
    expected_cid = "102492721"
    record = local_minter._find_record_by_identifier("sysid", previous_sysid)
    assert [record.type, record.identifier, record.cid] == ["sysid", previous_sysid, expected_cid]

    # test the CidMinter class
    cid = cid_minter.mint_cid(input_ids)

    assert "Find CID in Zephir Database by OCNs" not in caplog.text
    assert "Local minter: No CID found by SYSID" in caplog.text
    assert "Find CID in Zephir Database by SYSID" in caplog.text
    assert "Find CID in local minter by PREV_SYSID" in caplog.text
    assert f"Local minter error: Found more than one matched CID: {expected_cids} by PREV_SYSID" in caplog.text
    assert "Find CID in Zephir Database by PREV_SYSID" in caplog.text


def test_step_3_a_3(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 3a3: Found one matched CID by previous contribsys ID in local minter.
       Use this CID and do not perform Zephir DB search.
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    cid_minter = CidMinter(config)
    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    local_minter = LocalMinter(setup_local_minter["local_minter"])
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]

    input_ids= {"contribsys_ids": "test.12345", "previous_contribsys_ids": "pur215476", "htid": "hvd.hw5jdo"}

    # sysid|pur215476|002492721
    sysid = input_ids.get("contribsys_ids")
    previous_sysid = input_ids.get("previous_contribsys_ids")
    expected_cid = "002492721"

    # verify in local minter: sysid/cid are not in local minter
    record = local_minter._find_record_by_identifier("sysid", sysid)
    assert record is None

    # verify in local minter: previous sysid/cid are not in local minter
    record = local_minter._find_record_by_identifier("sysid", previous_sysid)
    assert [record.type, record.identifier, record.cid] == ["sysid", previous_sysid, expected_cid]

    # test the CidMinter class
    cid = cid_minter.mint_cid(input_ids)
    assert cid == expected_cid

    assert f"Local minter: Found matched CID: ['{expected_cid}'] by PREV_SYSID: ['{previous_sysid}']" in caplog.text
    assert "Find CID in Zephir Database by OCNs" not in caplog.text
    assert "Local minter: Record exists. No need to update" in caplog.text
    assert f"Updated local minter: contribsys id: {sysid}" in caplog.text
    assert f"Updated local minter: previous contribsys id: {previous_sysid}" in caplog.text


def test_step_3_b_1(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 2b1: Found one matched CID by previous contribsys IDs in Zephir.
       Use this CID and do not perform Zephir DB search.
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    cid_minter = CidMinter(config)
    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    local_minter = LocalMinter(setup_local_minter["local_minter"])
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]

    input_ids= {"contribsys_ids": "test.12345", "previous_contribsys_ids": "hvd000012735", "htid": "hvd.hw5jdo"}

    sysid = input_ids.get("contribsys_ids")
    SELECT_ZEPHIR_BY_SYSID = f"SELECT distinct cid, contribsys_id from zephir_records WHERE contribsys_id = '{sysid}'"

    # verify CID and sysids in Zephir DB: sysid/cid are not in zephir
    results = zephirDb._get_query_results(SELECT_ZEPHIR_BY_SYSID)
    assert len(results) == 0 

    previous_sysid = input_ids.get("previous_contribsys_ids")
    SELECT_ZEPHIR_BY_SYSID = f"SELECT distinct cid, contribsys_id from zephir_records WHERE contribsys_id = '{previous_sysid}'"

    # verify CID and previous_sysids in Zephir DB: previous_sysid/cid are in zephir
    expected_cid = "009705704"
    expected = [{"cid": expected_cid, "contribsys_id": previous_sysid}]
    results = zephirDb._get_query_results(SELECT_ZEPHIR_BY_SYSID)
    assert len(results) ==  1
    assert results == expected

    # verify in local minter: previous sysid not in local minter
    record = local_minter._find_record_by_identifier("sysid", previous_sysid)
    assert record is None

    # test the CidMinter class
    expected_cid = "009705704"
    cid = cid_minter.mint_cid(input_ids)
    assert cid == expected_cid

    assert f"Local minter: No CID found by PREV_SYSID: ['{previous_sysid}']" in caplog.text
    assert f"Zephir minter: Found matched CIDs: ['{expected_cid}'] by previous contribsys IDs: ['{previous_sysid}']" in caplog.text

    # verify in local minter: previous sysid/cid are in local minter now
    record = local_minter._find_record_by_identifier("sysid", sysid)
    assert [record.type, record.identifier, record.cid] == ["sysid", sysid, expected_cid] 

    record = local_minter._find_record_by_identifier("sysid", previous_sysid)
    assert [record.type, record.identifier, record.cid] == ["sysid", previous_sysid, expected_cid] 


def test_step_3_b_2(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 3b2: Found more than one matched CID by previous contribsys ID in Zephir.
       Report Error and reject the incoming record.
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    cid_minter = CidMinter(config)
    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    local_minter = LocalMinter(setup_local_minter["local_minter"])
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]

    input_ids= {"contribsys_ids": "test.12345", "previous_contribsys_ids": "acme.992222222", "htid": "hvd.hw5jdo"}

    sysid = input_ids.get("contribsys_ids")
    SELECT_ZEPHIR_BY_SYSID = f"SELECT distinct cid, contribsys_id from zephir_records WHERE contribsys_id = '{sysid}'"

    # verify CID and sysids in Zephir DB: sysid/cid are not in zephir
    results = zephirDb._get_query_results(SELECT_ZEPHIR_BY_SYSID)
    assert len(results) == 0 

    # 102359219|acme.992222222
    # 102359222|acme.992222222
    # 102359223|acme.992222222
    previous_sysid = input_ids.get("previous_contribsys_ids")
    SELECT_ZEPHIR_BY_SYSID = f"SELECT distinct cid, contribsys_id from zephir_records WHERE contribsys_id = '{previous_sysid}'"

    # verify CID and previous_sysids in Zephir DB: previous_sysid/cid are in zephir
    results = zephirDb._get_query_results(SELECT_ZEPHIR_BY_SYSID)
    assert len(results) == 3 

    # verify in local minter: previous sysid not in local minter
    record = local_minter._find_record_by_identifier("sysid", previous_sysid)
    assert record is None

    # test the CidMinter class
    with pytest.raises(Exception) as e_info:
        cid = cid_minter.mint_cid(input_ids)
        assert "ZED code: pr0042 - Record with previous local num matches more than one CID" in e_info

def test_step_3_b_3(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 3b3: Found a matched CID by previous contribsys ID in Zephir with error condition:
         - Zephir cluster contains records from different contrib systems. 
       Skip the matched CID and assign a new CID in this case.
    """
    caplog.set_level(logging.DEBUG)
    config = {
        "zephirdb_conn_str": setup_zephir_db["zephirDb"],
        "localdb_conn_str": setup_local_minter["local_minter"],
        "leveldb_primary_path": setup_leveldb["primary_db_path"],
        "leveldb_cluster_path": setup_leveldb["cluster_db_path"],
    }

    cid_minter = CidMinter(config)
    zephirDb = ZephirDatabase(setup_zephir_db["zephirDb"])
    local_minter = LocalMinter(setup_local_minter["local_minter"])
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]

    input_ids= {"contribsys_ids": "test.12345", "previous_contribsys_ids": "acme.b2222222", "htid": "test.12345"}

    sysid = input_ids.get("contribsys_ids")
    SELECT_ZEPHIR_BY_SYSID = f"SELECT distinct cid, contribsys_id from zephir_records WHERE contribsys_id = '{sysid}'"

    # verify CID and sysids in Zephir DB: sysid/cid are not in zephir
    results = zephirDb._get_query_results(SELECT_ZEPHIR_BY_SYSID)
    assert len(results) == 0

    # 102359219|acme.992222222
    # 102359219|acme.b2222222

    previous_sysid = input_ids.get("previous_contribsys_ids")
    SELECT_ZEPHIR_BY_SYSID = f"SELECT distinct cid, contribsys_id from zephir_records WHERE contribsys_id = '{previous_sysid}'"

    # verify CID and previous_sysids in Zephir DB: previous_sysid/cid are in zephir
    expected_cid = "102359219"
    expected = [{"cid": expected_cid, "contribsys_id": previous_sysid}]
    results = zephirDb._get_query_results(SELECT_ZEPHIR_BY_SYSID)
    assert len(results) ==  1
    assert results == expected

    # verify in local minter: previous sysid not in local minter
    record = local_minter._find_record_by_identifier("sysid", previous_sysid)
    assert record is None

    SELECT_MINTER = "SELECT cid from cid_minter"
    results = zephirDb._get_query_results(SELECT_MINTER)
    assert len(results) == 1
    minter = results[0].get("cid")

    # test the CidMinter class
    cid = cid_minter.mint_cid(input_ids)
    # minted a new CID
    assert int(cid) == int(minter) + 1 

    assert f"Zephir minter: Found matched CIDs: ['{expected_cid}'] by previous contribsys IDs: ['{previous_sysid}']" in caplog.text
    assert f"Zephir cluster contains records from different contrib systems. Skip this CID ({expected_cid}) assignment" in caplog.text
    assert f"Minted a new minter: {cid} - from current minter" in caplog.text
    assert "Local minter: Inserted a new record" in caplog.text
    assert f"Updated local minter: contribsys id: {sysid}" in caplog.text
    assert f"Updated local minter: previous contribsys id: {previous_sysid}" in caplog.text

    # verify local minter has been updated
    for id in [sysid, previous_sysid]:
        record = local_minter._find_record_by_identifier("sysid", id)
        assert [record.type, record.identifier, record.cid] == ["sysid", id, cid]

    # verify cid_minter has been updated
    results = zephirDb._get_query_results(SELECT_MINTER)
    assert len(results) == 1
    minter_new = results[0].get("cid")
    assert int(minter_new) == int(minter) + 1


# FIXTURES
@pytest.fixture
def setup_leveldb(tmpdatadir, csv_to_df_loader):
    dfs = csv_to_df_loader
    primary_db_path = create_primary_db(tmpdatadir, dfs["primary.csv"])
    cluster_db_path = create_cluster_db(tmpdatadir, dfs["primary.csv"])
    os.environ["OVERRIDE_PRIMARY_DB_PATH"] = primary_db_path
    os.environ["OVERRIDE_CLUSTER_DB_PATH"] = cluster_db_path

    return {
        "tmpdatadir": tmpdatadir,
        "dfs": dfs,
        "primary_db_path": primary_db_path,
        "cluster_db_path": cluster_db_path
    }

@pytest.fixture
def setup_zephir_db(data_dir, tmpdir, scope="session"):
    db_name = "test_db_for_zephir.db"
    #database = os.path.join(tmpdir, db_name)
    database = os.path.join(data_dir, db_name)
    setup_sql = os.path.join(data_dir, "setup_zephir_test_db.sql")

    cmd = "sqlite3 {} < {}".format(database, setup_sql)
    os.system(cmd)

    db_conn_str = 'sqlite:///{}'.format(database)
    os.environ["OVERRIDE_DB_CONNECT_STR"] = db_conn_str

    return {
        "zephirDb": db_conn_str
    }

@pytest.fixture
def setup_local_minter(data_dir, tmpdir, scope="session"):
    db_name = "test_minter_sqlite.db"
    #database = os.path.join(tmpdir, db_name)
    database = os.path.join(data_dir, db_name)
    prepare_datasets = os.path.join(data_dir, "prepare_cid_minter_datasets.sql")

    cmd = "sqlite3 {} < {}".format(database, prepare_datasets)
    os.system(cmd)

    db_conn_str = 'sqlite:///{}'.format(database)
    os.environ["OVERRIDE_DB_CONNECT_STR"] = db_conn_str

    return {"local_minter": db_conn_str}

# HELPERS
def int_to_bytes(inum):
    return inum.to_bytes((inum.bit_length() + 7) // 8, "big")


def int_from_bytes(bnum):
    return int.from_bytes(bnum, "big")


def create_primary_db(path, df):
    """Create a primary ocn lookup LevelDB database based with test data

    Note:
        1) Expects a dataframe: [ocn, primary]

    Args:
        Path: Database path
        df: Pandas dataframe of test data [ocn, primary]

    Returns:
        Path to the LevelDB database

    """
    db_path = os.path.join(path, "primary/")
    db = plyvel.DB(db_path, create_if_missing=True)

    df = df.sort_values(by=["ocn"])
    ocn_pos = df.columns.get_loc("ocn") + 1
    primary_pos = df.columns.get_loc("primary") + 1

    for row in df.itertuples():
        db.put(int_to_bytes(row[ocn_pos]), int_to_bytes(row[primary_pos]))
    db.close()
    return db_path

def create_cluster_db(path, df):
    """Create a cluster ocns lookup LevelDB database based with test data

    Note:
        1) Expects a dataframe: [ocn, primary]
        2) Produces a LevelDB with key(primary) and value([ocns,...])
        3) Primary-only clusters are excluded

    Args:
        Path: Database path
        df: Pandas dataframe of test data [ocn, primary]

    Returns:
        Path to the LevelDB database

    """
    db_path = os.path.join(path, "cluster/")
    db = plyvel.DB(db_path, create_if_missing=True)

    packer = msgpack.Packer()

    df = df.sort_values(by=["primary","ocn"])
    ocn_pos = df.columns.get_loc("ocn") + 1
    primary_pos = df.columns.get_loc("primary") + 1

    current_primary = 0
    cluster = []
    for row in df.itertuples():
        if row[primary_pos] != current_primary:
            if current_primary != 0:
                if len(cluster) > 0:
                    db.put(int_to_bytes(current_primary), packer.pack(cluster))
            current_primary = row[primary_pos]
            cluster = []
        if current_primary != row[ocn_pos]:
            cluster.append(row[ocn_pos])
    if len(cluster) > 0:
        db.put(int_to_bytes(current_primary), packer.pack(cluster))
    db.close()
    return db_path
