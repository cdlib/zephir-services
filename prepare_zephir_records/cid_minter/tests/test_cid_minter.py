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

def test_cid_minter_step_0_1(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
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

def test_cid_minter_step_0_2(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
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

def test_cid_minter_step_1_b_1(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 1b1: Find matched cluster by OCNs in Zephir DB.
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

    record = local_minter._find_record_by_identifier("sysid", "hvd000012735")
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

    record = local_minter._find_record_by_identifier("sysid", "hvd000012735")
    assert [record.type, record.identifier, record.cid] == ["sysid", "hvd000012735", expected_cid]

def test_cid_minter_step_1_b_2(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 1b2: Find matched cluster by OCNs in Zephir DB.
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
    """Test case 1b3: Find matched cluster by OCNs in Zephir DB.
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


def test_step_1_a_1(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 1a: Find matched CID by OCNs in Local Minter.
       Also verifies workflow and error conditions:
         - Local minter: found CID found by OCN
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

def test_step_2_b_1(caplog, setup_leveldb, setup_zephir_db, setup_local_minter):
    """Test case 2b1: Find one matched CID by contribsys ID in Zephir.
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
    """Test case 2b2: Find more than one matched CID by contribsys IDs in Zephir.
       Also verifies workflow and error conditions:
         - found more than one matched CID in Zephir DB
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
    expected_cid = "000641789"  # the lower one
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


"""Test cid_inquiry_by_ocns() function which returns a dict with: 
  "iquiry_ocns": input ocns, list of integers.
  "matched_oclc_clusters": OCNs in matched OCLC clusters, list of lists in integers
  "num_of_matched_oclc_clusters": number of matched OCLC clusters#
  "inquiry_ocns_zephir": ocns used to quesry Zephir clusters
  "cid_ocn_list": list of cid and ocn tuples from DB query
  "cid_ocn_clusters": dict with key="cid", value=list of ocns in the cid cluster,
  "num_of_matched_zephir_clusters": num of matched Zephir clusters
  "min_cid": the lowest CID among the clusters

  Test cases are documented in Google drive under D2D/Zephir/Developers
  Filename: CID Minting Test Cases Using OCLC Concordance Table:
  Format: Word
  Note: only contain cases for matching by OCNS
  Last modified: 08/25/2020
  URL: https://docs.google.com/document/d/1rjd0NZfiSDLR7Kk4PdaWn8CLoZltJSUPPl_5pn3CAjA/edit#heading=h.yh7pb678uhgs

  Filename: CID_Minting_Test_Cases_Using_OCLC_Concordance_Table
  Format: Google  Sheet
  Note:  contain ases for matching by OCNS and by local sysid and previous sysid
  Last modified: 5/22/2020
  URL: https://docs.google.com/spreadsheets/d/1Oak1NjbrxX7oP9hRpmqKBgwxAAHTyGw6zUkJ3DwXLok/edit#gid=0
"""

# Test case 1 & 2: Incoming record matches a single primary record in the Concordance Table.
## a. Record OCN + Concordance OCN(s) matches no CID
def xxx_case_1_a_i_ii(setup_leveldb, setup_zephir_db):
    """1. Incoming record contains one OCN that matches a single Concordance Table primary record.
       a. Record OCN + Concordance OCN(s) matches no CID
       i. Concordance primary record has one OCN (equals to Record OCN)
       ii. Concordance primary record has more than one OCNs

       Test datasets:
         OCLC Cluster with one OCN: [1000000000]
         OCLC Cluster with more than one OCNs: [123, 18329830, 67524283]

         Incoming record OCN:
           for i: 1000000000
           for ii: 18329830
    """
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]
    zephirDb = setup_zephir_db["zephirDb"]

    incoming_ocns_list = {
        "i_one_ocn_cluster": [1000000000],
        "ii_multiple_ocns_cluster": [18329830],
    }

    expected_oclc_clusters = {
        "i_one_ocn_cluster": [[1000000000]],
        "ii_multiple_ocns_cluster": [[123, 18329830, 67524283]],
    }
    inquiry_ocns_zephir = {
        "i_one_ocn_cluster": [1000000000],
        "ii_multiple_ocns_cluster": [123, 18329830, 67524283],
    }
    expected_cid_ocn_list = []
    expected_zephir_clsuters = {}

    for k, incoming_ocns in incoming_ocns_list.items():
        result = cid_inquiry_by_ocns(incoming_ocns, zephirDb, primary_db_path, cluster_db_path)
        assert result["inquiry_ocns"] == incoming_ocns
        assert result["matched_oclc_clusters"] == expected_oclc_clusters[k]
        assert result["num_of_matched_oclc_clusters"] == 1
        assert result["inquiry_ocns_zephir"] == inquiry_ocns_zephir[k]
        assert result["cid_ocn_list"] == expected_cid_ocn_list
        assert result["cid_ocn_clusters"] == expected_zephir_clsuters
        assert result["num_of_matched_zephir_clusters"] == 0


def xxx_case_2_a_i_ii(setup_leveldb, setup_zephir_db):
    """2. Incoming record contains 2+ OCNs that matches a single Concordance Table primary record.
       a. Record OCN + Concordance OCN(s) matches no CID
       i. Concordance primary record has one OCN (equals to Record OCN)
       ii. Concordance primary record has more than one OCNs

       Test datasets:
         OCLC Cluster with one OCN: [100000001]
         OCLC Cluster with more than one OCNs: [1234, 976940347]

         Incoming record OCN:
           for i: [100000001, 123456789010] (1 primary ocn + 1 invalid ocn)
           for ii: [976940347, 123456789020, 123456789010] (1 other ocn + 2 invalid ocns)
    """
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]
    zephirDb = setup_zephir_db["zephirDb"]

    incoming_ocns_list = {
        "i_one_ocn_cluster": [100000001, 123456789010],
        "ii_multiple_ocns_cluster": [976940347, 123456789020, 123456789010],
    }

    expected_oclc_clusters = {
        "i_one_ocn_cluster": [[100000001]],
        "ii_multiple_ocns_cluster": [[1234, 976940347]],
    }
    inquiry_ocns_zephir = {
        "i_one_ocn_cluster": [100000001, 123456789010],
        "ii_multiple_ocns_cluster": [1234, 976940347, 123456789010, 123456789020],
    }
    expected_cid_ocn_list = []
    expected_zephir_clsuters = {}

    for k, incoming_ocns in incoming_ocns_list.items():
        result = cid_inquiry_by_ocns(incoming_ocns, zephirDb, primary_db_path, cluster_db_path)
        assert result["inquiry_ocns"] == incoming_ocns
        assert result["matched_oclc_clusters"] == expected_oclc_clusters[k]
        assert result["num_of_matched_oclc_clusters"] == 1
        assert result["inquiry_ocns_zephir"] == inquiry_ocns_zephir[k]
        assert result["cid_ocn_list"] == expected_cid_ocn_list
        assert result["cid_ocn_clusters"] == expected_zephir_clsuters
        assert result["num_of_matched_zephir_clusters"] == 0


## b. Record OCN + Concordance OCN(s) matches one CID
def xxx_case_1_b_i(setup_leveldb, setup_zephir_db):
    """ Test case 1.b.i.1):
        1. Incoming record contains one OCN that matches a single Concordance Table primary record
        b. Record OCN + Concordance OCN(s) matches one CID
        i. Concordance primary record has one OCN (equals to Record OCN)
        1). Matched Zephir cluster contains the OCN

        Test datasets:
        Zephir cluster: CID: 000249880; OCN: 999531
        OCLC primary OCN: 999531; other OCNs: None
        Incoming ocn: 999531
    """
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]
    zephirDb = setup_zephir_db["zephirDb"]

    incoming_ocns = [999531]
    expected_oclc_clusters = [[999531]]
    expected_cid_ocn_list = [{"cid": '000249880', "ocn": '999531'}]
    expected_zephir_clsuters = {
        "000249880": ['999531'],
    }
    result = cid_inquiry_by_ocns(incoming_ocns, zephirDb, primary_db_path, cluster_db_path)

    print(result["matched_oclc_clusters"])
    print(result["cid_ocn_clusters"])
    print(result["inquiry_ocns_zephir"])

    assert result["inquiry_ocns"] == incoming_ocns
    assert result["matched_oclc_clusters"] == expected_oclc_clusters
    assert result["num_of_matched_oclc_clusters"] == 1
    assert result["inquiry_ocns_zephir"] == incoming_ocns
    assert result["cid_ocn_list"] == expected_cid_ocn_list
    assert result["cid_ocn_clusters"] == expected_zephir_clsuters
    assert result["num_of_matched_zephir_clusters"] == 1

def xxx_case_1_b_ii_1_and_2(setup_leveldb, setup_zephir_db):
    """ Test case 1.b.i.1):
        1. Incoming record contains one OCN that matches a single Concordance Table primary record
        b. Record OCN + Concordance OCN(s) matches one CID
        ii. Concordance primary record has more than one OCNs
        1). Zephir cluster contains the Record OCN
        2). Zephir cluster doesn't have the Record OCN

        Test datasets:
        Zephir cluster:
        CID: 009547317; OCNs: 33393343, 28477569

        OCLC Primary OCN: 33393343
        Others OCNs: 28477569, 44192417

        Incoming OCN for test case:
          1) 33393343 - Zephir cluster contains the Record OCN
          2) 44192417 - Zephir cluster doesn't have the Record OCN
    """
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]
    zephirDb = setup_zephir_db["zephirDb"]

    incoming_ocns_list = {
        "case_1_zephir_has_record_ocn": [33393343], 
        "case_2_zephir_does_not_have_record_ocn": [44192417],
    }

    expected_oclc_clusters = [[28477569, 33393343, 44192417]]
    inquiry_ocns_zephir = [28477569, 33393343, 44192417]
    expected_cid_ocn_list = [{"cid": '009547317', "ocn": '28477569'}, {"cid": '009547317', "ocn": '33393343'}]
    expected_zephir_clsuters = {
        "009547317": ['28477569', '33393343'],
    }

    for k, incoming_ocns in incoming_ocns_list.items():
        result = cid_inquiry_by_ocns(incoming_ocns, zephirDb, primary_db_path, cluster_db_path)
        assert result["inquiry_ocns"] == incoming_ocns
        assert result["matched_oclc_clusters"] == expected_oclc_clusters
        assert result["num_of_matched_oclc_clusters"] == 1
        assert result["inquiry_ocns_zephir"] == inquiry_ocns_zephir
        assert result["cid_ocn_list"] == expected_cid_ocn_list
        assert result["cid_ocn_clusters"] == expected_zephir_clsuters
        assert result["num_of_matched_zephir_clusters"] == 1

def xxx_case_2_b_i(setup_leveldb, setup_zephir_db):
    """ Test case 2.b.i.1):
        1. Incoming record contains 2+ OCNs that matches a single Concordance Table primary record
        b. Record OCN + Concordance OCN(s) matches one CID
        i. Concordance primary record has one OCN (equals to Record OCN)
        1). Matched Zephir cluster contains the OCN

        Test datasets:
        Zephir cluster: CID: 000249880; OCN: 999531
        OCLC primary OCN: 999531; other OCNs: None
        Incoming ocn: 999531, 123456789030
    """
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]
    zephirDb = setup_zephir_db["zephirDb"]

    incoming_ocns = [999531, 123456789030]
    expected_oclc_clusters = [[999531]]
    expected_cid_ocn_list = [{"cid": '000249880', "ocn": '999531'}]
    expected_zephir_clsuters = {
        "000249880": ['999531'],
    }
    result = cid_inquiry_by_ocns(incoming_ocns, zephirDb, primary_db_path, cluster_db_path)

    assert result["inquiry_ocns"] == incoming_ocns
    assert result["matched_oclc_clusters"] == expected_oclc_clusters
    assert result["num_of_matched_oclc_clusters"] == 1
    assert result["inquiry_ocns_zephir"] == incoming_ocns
    assert result["cid_ocn_list"] == expected_cid_ocn_list
    assert result["cid_ocn_clusters"] == expected_zephir_clsuters
    assert result["num_of_matched_zephir_clusters"] == 1

def xxx_case_2_b_ii_1_and_2(setup_leveldb, setup_zephir_db):
    """ Test case 2.b.i.1):
        1. Incoming record contains 2+ OCNs that matches a single Concordance Table primary record
        b. Record OCN + Concordance OCN(s) matches one CID
        ii. Concordance primary record has more than one OCNs
        1). Zephir cluster contains the Record OCN
        2). Zephir cluster doesn't have the Record OCN

        Test datasets:
        Zephir cluster:
        CID: 009547317; OCNs: 33393343, 28477569

        OCLC Primary OCN: 33393343
        Others OCNs: 28477569, 44192417

        Incoming OCN for test case:
          1) 33393343, 28477569 - Zephir cluster contains the Record OCN
          2) 44192417, 123456789040 - Zephir cluster doesn't have the Record OCN
    """
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]
    zephirDb = setup_zephir_db["zephirDb"]

    incoming_ocns_list = {
        "case_1_zephir_has_record_ocn": [33393343, 28477569], 
        "case_2_zephir_does_not_have_record_ocn": [44192417, 123456789040],
    }

    expected_oclc_clusters = [[28477569, 33393343, 44192417]]
    inquiry_ocns_zephir = {
        "case_1_zephir_has_record_ocn": [28477569, 33393343, 44192417],
        "case_2_zephir_does_not_have_record_ocn": [28477569, 33393343, 44192417, 123456789040],
    }
    expected_cid_ocn_list = [{"cid": '009547317', "ocn": '28477569'}, {"cid": '009547317', "ocn": '33393343'}]
    expected_zephir_clsuters = {
        "009547317": ['28477569', '33393343'],
    }
    expected_min_cid = "009547317"

    for k, incoming_ocns in incoming_ocns_list.items():
        result = cid_inquiry_by_ocns(incoming_ocns, zephirDb, primary_db_path, cluster_db_path)
        assert result["inquiry_ocns"] == incoming_ocns
        assert result["matched_oclc_clusters"] == expected_oclc_clusters
        assert result["num_of_matched_oclc_clusters"] == 1
        assert result["inquiry_ocns_zephir"] == inquiry_ocns_zephir[k]
        assert result["cid_ocn_list"] == expected_cid_ocn_list
        assert result["cid_ocn_clusters"] == expected_zephir_clsuters
        assert result["num_of_matched_zephir_clusters"] == 1
        assert result["min_cid"] == expected_min_cid


## Test case c: incoming record matches 2+ CID
def xxx_case_1_and_2_c(setup_leveldb, setup_zephir_db):
    """ Test case 2.b.i.1):
        1. Incoming record matches a single Concordance Table primary record
        c. Record OCN + Concordance OCN(s) matches 2+ CID

        Test datasets:
        Zephir cluster:
        CID 1: 002492721, OCNs: [8727632];        
        CID 2: 000000280, OCNs: [217211158, 25909]

        OCLC OCNs: 
            [8727632, 24253253], 
            [25909, 633478297, 976588742, 1063434341] - incoming ocns will not match on this cluster.

        Incoming OCN for test case:
          [217211158 (invalid), 8727632]
    """
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]
    zephirDb = setup_zephir_db["zephirDb"]

    incoming_ocns = [217211158, 8727632] 

    expected_oclc_clusters = [[8727632, 24253253]]
    inquiry_ocns_zephir = [8727632, 24253253, 217211158] 

    expected_cid_ocn_list = [
            {"cid": '000000280', "ocn": '217211158'}, 
            {"cid": '000000280', "ocn": '25909'}, 
            {"cid": '002492721', "ocn":'8727632'}]
    expected_zephir_clsuters = {
        "000000280": ['217211158', '25909'],
        "002492721": ['8727632'],
    }
    expected_min_cid = "000000280"

    result = cid_inquiry_by_ocns(incoming_ocns, zephirDb, primary_db_path, cluster_db_path)
    print(result)

    assert result["inquiry_ocns"] == incoming_ocns
    assert result["matched_oclc_clusters"] == expected_oclc_clusters
    assert result["num_of_matched_oclc_clusters"] == 1
    assert result["inquiry_ocns_zephir"] == inquiry_ocns_zephir
    assert result["cid_ocn_list"] == expected_cid_ocn_list
    assert result["cid_ocn_clusters"] == expected_zephir_clsuters
    assert result["num_of_matched_zephir_clusters"] == 2 
    assert result["min_cid"] ==  expected_min_cid

# Test case 3 - "confused record"
# Incoming record contains 2+ OCNs that resolve to two different master records in the Concordance Table
## a. Record OCNs + OCLC OCNs match no CID
def xxx_case_3_a(setup_leveldb, setup_zephir_db):
    """ Test case 3.a:
        3. Incoming record contains 2+ OCNs that resolve to two Concordance Table primary record
        a. Record OCNs + OCLC OCNs match no CID

        Test datasets:
        Zephir cluster: no matches

        OCLC OCNs: 
            [100], 
            [300, 39867290, 39867383].

        Incoming OCN for test case:
          [100, 300]
    """
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]
    zephirDb = setup_zephir_db["zephirDb"]

    incoming_ocns = [100, 300] 

    expected_oclc_clusters = [[300, 39867290, 39867383], [100]]
    inquiry_ocns_zephir = [100, 300, 39867290, 39867383] 

    expected_cid_ocn_list = []
    expected_zephir_clsuters = {}
    expected_min_cid = None

    result = cid_inquiry_by_ocns(incoming_ocns, zephirDb, primary_db_path, cluster_db_path)
    print(result)

    assert result["inquiry_ocns"] == incoming_ocns
    assert result["matched_oclc_clusters"] == expected_oclc_clusters
    assert result["num_of_matched_oclc_clusters"] == 2 
    assert result["inquiry_ocns_zephir"] == inquiry_ocns_zephir
    assert result["cid_ocn_list"] == expected_cid_ocn_list
    assert result["cid_ocn_clusters"] == expected_zephir_clsuters
    assert result["num_of_matched_zephir_clusters"] == 0
    assert result["min_cid"] ==  expected_min_cid

## b. Record OCNs + OCLC OCNs match one CID
def xxx_case_3_b(setup_leveldb, setup_zephir_db):
    """ Test case 3.b:
        3. Incoming record contains 2+ OCNs that resolve to two Concordance Table primary record
        b. Record OCNs + OCLC OCNs match one CID

        Test datasets:
        Zephir cluster: one match
        CID: 008648991
        OCNs: 4912741, 5066412, 23012053

        OCLC OCNs:
            [200, 1078101879, 1102728950, etc.] (only selected a subset for testing)
            [4912741, 5066412, 23012053, 228676186, 315449541, etc.] (only selected a subset for testing) 

        Incoming OCN for test case:
          [200, 228676186]
    """
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]
    zephirDb = setup_zephir_db["zephirDb"]

    incoming_ocns = [200, 228676186]

    expected_oclc_clusters = [[4912741, 5066412, 23012053, 228676186, 315449541], [200, 1078101879, 1102728950]]
    inquiry_ocns_zephir = [200, 4912741, 5066412, 23012053, 228676186, 315449541, 1078101879, 1102728950]

    expected_cid_ocn_list = [
            {"cid": '008648991', "ocn": '23012053'}, 
            {"cid": '008648991', "ocn": '4912741'}, 
            {"cid": '008648991', "ocn": '5066412'}]
    expected_zephir_clsuters = {"008648991": ['23012053', '4912741', '5066412']}
    expected_min_cid =  "008648991"

    result = cid_inquiry_by_ocns(incoming_ocns, zephirDb, primary_db_path, cluster_db_path)
    print(result)

    assert result["inquiry_ocns"] == incoming_ocns
    assert result["matched_oclc_clusters"] == expected_oclc_clusters
    assert result["num_of_matched_oclc_clusters"] == 2
    assert result["inquiry_ocns_zephir"] == inquiry_ocns_zephir
    assert result["cid_ocn_list"] == expected_cid_ocn_list
    assert result["cid_ocn_clusters"] == expected_zephir_clsuters
    assert result["num_of_matched_zephir_clusters"] == 1 
    assert result["min_cid"] ==  expected_min_cid


def xxx_case_3_c(setup_leveldb, setup_zephir_db):
    """ Test case 3.c:
        3. Incoming record contains 2+ OCNs that resolve to two Concordance Table primary record
        c. Record OCNs + OCLC OCNs match two CIDs

        Test datasets:
        Zephir cluster: 2 matches
        cid 1: 000002076, ocns= 2094039, 241092814, 140869; 
        cid 2: 102337772, ocn=1008263420 (created in Dev for testing)

        OCLC OCNs:
        [140869, 1150810243],  
        [2094039, 1008263420]

        Incoming OCN for test case:
        [140869, 2094039 (matches 2 CIDs)] 
    """
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]
    zephirDb = setup_zephir_db["zephirDb"]

    incoming_ocns = [140869, 2094039]

    expected_oclc_clusters = [[2094039, 1008263420], [140869, 1150810243]]
    inquiry_ocns_zephir = [140869,  2094039,  1008263420, 1150810243]

    expected_cid_ocn_list = [
            {"cid": '000002076', "ocn": '140869'}, 
            {"cid": '000002076', "ocn": '2094039'}, 
            {"cid": '000002076', "ocn": '241092814'},
            {"cid": '102337772', "ocn": '1008263420'}
            ]
    expected_zephir_clsuters = {
            "000002076": ['140869', '2094039', '241092814'],
            "102337772": ['1008263420']
            }
    expected_min_cid =  "000002076"

    result = cid_inquiry_by_ocns(incoming_ocns, zephirDb, primary_db_path, cluster_db_path)
    print(result)

    assert result["inquiry_ocns"] == incoming_ocns
    assert result["matched_oclc_clusters"] == expected_oclc_clusters
    assert result["num_of_matched_oclc_clusters"] == 2
    assert result["inquiry_ocns_zephir"] == inquiry_ocns_zephir
    assert result["cid_ocn_list"] == expected_cid_ocn_list
    assert result["cid_ocn_clusters"] == expected_zephir_clsuters
    assert result["num_of_matched_zephir_clusters"] == 2
    assert result["min_cid"] ==  expected_min_cid

# Test case 4. Incoming record contains OCNs that resolve to nothing in the Concordance Table
def xxx_case_4_abc(setup_leveldb, setup_zephir_db):
    """ Test case 4:
        4. Incoming record contains OCNs that resolve to nothing in the Concordance Table
        a. Record OCNs match no CID
        b. Record OCNs match one CID
        c. Record OCNs matches 2+ CIDs

        Test datasets:
        Zephir clusters: 
        cid=102337774	ocn: 1234567890102
        cid=102337775	ocn: 1234567890101
        cid=102337776	ocn: 1234567890103

        OCLC OCNs: No

        Incoming OCN for test cases:
        matches no Zephir CID: [1234567890104] 
        matches 1 zephir CID: [1234567890101]
        mathces 2 Zephir CIDs: [1234567890102, 1234567890103]
    """
    primary_db_path = setup_leveldb["primary_db_path"]
    cluster_db_path = setup_leveldb["cluster_db_path"]
    zephirDb = setup_zephir_db["zephirDb"]

    incoming_ocns_list = {
        "no_cid": [1234567890104],
        "1_cid": [1234567890101],
        "2_cids": [1234567890102, 1234567890103],
    }

    expected_oclc_clusters = []
    inquiry_ocns_zephir = {
        "no_cid": [1234567890104],
        "1_cid": [1234567890101],
        "2_cids": [1234567890102, 1234567890103],
     }
    expected_cid_ocn_list = {
        "no_cid": [],
        "1_cid": [{"cid": '102337775', "ocn": '1234567890101'}],
        "2_cids": [{"cid": '102337774', "ocn": '1234567890102'}, {"cid": '102337776', "ocn": '1234567890103'}],
    }
    expected_zephir_clsuters = {
        "no_cid": {},
        "1_cid": {"102337775": ['1234567890101']},
        "2_cids": {
            "102337774": ['1234567890102'],
            "102337776": ['1234567890103'],
        }
    }
    expected_num_of_zephir_clsuters = {
        "no_cid": 0,
        "1_cid":  1,
        "2_cids": 2, 
    }
    expected_min_cid = {
            "no_cid": None,
            "1_cid": '102337775',
            "2_cids": '102337774',
    }
    for k, incoming_ocns in incoming_ocns_list.items():
        result = cid_inquiry_by_ocns(incoming_ocns, zephirDb, primary_db_path, cluster_db_path)
        assert result["inquiry_ocns"] == incoming_ocns
        assert result["matched_oclc_clusters"] == expected_oclc_clusters
        assert result["num_of_matched_oclc_clusters"] == 0 
        assert result["inquiry_ocns_zephir"] == inquiry_ocns_zephir[k]
        assert result["cid_ocn_list"] == expected_cid_ocn_list[k]
        assert result["cid_ocn_clusters"] == expected_zephir_clsuters[k]
        assert result["num_of_matched_zephir_clusters"] == expected_num_of_zephir_clsuters[k] 
        assert result["min_cid"] ==  expected_min_cid[k]

def xxx_flat_and_dedup_sort_list():
    input_list = {
        "1_signle_item_list": [[1]],
        "2_signle_item_lists": [[234], [1], [234]],
        "3_multi_items_lists": [[567, 123], [2], [1]],
        "empty_list": [],
    }
    expected = {
        "1_signle_item_list": [1],
        "2_signle_item_lists": [1, 234],
        "3_multi_items_lists": [1, 2, 123, 567],
        "empty_list": [],
    }
    for k, val in input_list.items():
        assert expected[k] == flat_and_dedup_sort_list(val)

def xxx_convert_comma_separated_str_to_int_list():
    input_list = {
        "1_item": "1",
        "2_items": "1,123",
        "empty_item_1": ",123",
        "empty_item_2": "123,",
        "empty_item_3": "1,,123",
        "empty_item_4": "",
        "empty_item_5": " ",
        "empty_item_6": ",",
        "error_1": "-123",
        "error_2": "ebook)ocm76968450",
        "error_3": "ebook)ocm76968450,123",
        "error_4": "0",
        "error_5": "0,-123",
        "error_6": "abc,xyz",
        "none": None
    }
    expected = {
        "1_item": [1],
        "2_items": [1, 123],
        "empty_item_1": [123],
        "empty_item_2": [123],
        "empty_item_3": [1,123],
        "empty_item_4": [],
        "empty_item_5": [],
        "empty_item_6": [],
        "error_1": [],
        "error_2": [],
        "error_3": [123],
        "error_4": [],
        "error_5": [],
        "error_6": [],
        "none": []
    }
    for k, val in input_list.items():
        assert expected[k] == convert_comma_separated_str_to_int_list(val)

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
