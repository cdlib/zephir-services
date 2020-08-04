import os

import msgpack
import pytest
import plyvel

from cid_inquiry import cid_inquiry
from cid_inquiry import flat_and_dedup_sort_list

"""Test cid_inquiry() function which returns a dict with: 
  "iquiry_ocns": input ocns, list of integers.
  "matched_oclc_clusters": OCNs in matched OCLC clusters, list of lists in integers
  "num_of_matched_oclc_clusters": number of matched OCLC clusters#
  "inquiry_ocns_zephir": ocns used to quesry Zephir clusters
  "cid_ocn_list": list of cid and ocn tuples from DB query
  "cid_ocn_clusters": dict with key="cid", value=list of ocns in the cid cluster,
  "num_of_matched_zephir_clusters": num of matched Zephir clusters
"""

# Test case 1 & 2: Incoming record matches a single primary record in the Concordance Table.
## a. Record OCN + Concordance OCN(s) matches no CID
def test_case_1_a_i_ii(setup_leveldb, setup_sqlite):
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
    primarydb_path = setup_leveldb["primarydb_path"]
    clusterdb_path = setup_leveldb["clusterdb_path"]
    db_conn_str = setup_sqlite["db_conn_str"]

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
        result = cid_inquiry(incoming_ocns, db_conn_str, primarydb_path, clusterdb_path)
        assert result["inquiry_ocns"] == incoming_ocns
        assert result["matched_oclc_clusters"] == expected_oclc_clusters[k]
        assert result["num_of_matched_oclc_clusters"] == 1
        assert result["inquiry_ocns_zephir"] == inquiry_ocns_zephir[k]
        assert result["cid_ocn_list"] == expected_cid_ocn_list
        assert result["cid_ocn_clusters"] == expected_zephir_clsuters
        assert result["num_of_matched_zephir_clusters"] == 0


def test_case_2_a_i_ii(setup_leveldb, setup_sqlite):
    """2. Incoming record contains 2+ OCNs that matches a single Concordance Table primary record.
       a. Record OCN + Concordance OCN(s) matches no CID
       i. Concordance primary record has one OCN (equals to Record OCN)
       ii. Concordance primary record has more than one OCNs

       Test datasets:
         OCLC Cluster with one OCN: [100000001]
         OCLC Cluster with more than one OCNs: [1234, 976940347]

         Incoming record OCN:
           for i: [100000001, 1234567890]
           for ii: [976940347, 12345678902, 12345678901] (1 other ocn + 2 invalid ocns)
    """
    primarydb_path = setup_leveldb["primarydb_path"]
    clusterdb_path = setup_leveldb["clusterdb_path"]
    db_conn_str = setup_sqlite["db_conn_str"]

    incoming_ocns_list = {
        "i_one_ocn_cluster": [100000001, 1234567890],
        "ii_multiple_ocns_cluster": [976940347, 12345678902, 12345678901],
    }

    expected_oclc_clusters = {
        "i_one_ocn_cluster": [[100000001]],
        "ii_multiple_ocns_cluster": [[1234, 976940347]],
    }
    inquiry_ocns_zephir = {
        "i_one_ocn_cluster": [100000001, 1234567890],
        "ii_multiple_ocns_cluster": [1234, 976940347, 12345678901, 12345678902],
    }
    expected_cid_ocn_list = []
    expected_zephir_clsuters = {}

    for k, incoming_ocns in incoming_ocns_list.items():
        result = cid_inquiry(incoming_ocns, db_conn_str, primarydb_path, clusterdb_path)
        assert result["inquiry_ocns"] == incoming_ocns
        assert result["matched_oclc_clusters"] == expected_oclc_clusters[k]
        assert result["num_of_matched_oclc_clusters"] == 1
        assert result["inquiry_ocns_zephir"] == inquiry_ocns_zephir[k]
        assert result["cid_ocn_list"] == expected_cid_ocn_list
        assert result["cid_ocn_clusters"] == expected_zephir_clsuters
        assert result["num_of_matched_zephir_clusters"] == 0


## b. Record OCN + Concordance OCN(s) matches one CID
def test_case_1_b_i(setup_leveldb, setup_sqlite):
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
    primarydb_path = setup_leveldb["primarydb_path"]
    clusterdb_path = setup_leveldb["clusterdb_path"]
    db_conn_str = setup_sqlite["db_conn_str"]

    incoming_ocns = [999531]
    expected_oclc_clusters = [[999531]]
    expected_cid_ocn_list = [('000249880', '999531')]
    expected_zephir_clsuters = {
        "000249880": ['999531'],
    }
    result = cid_inquiry(incoming_ocns, db_conn_str, primarydb_path, clusterdb_path)

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

def test_case_1_b_ii_1_and_2(setup_leveldb, setup_sqlite):
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
    primarydb_path = setup_leveldb["primarydb_path"]
    clusterdb_path = setup_leveldb["clusterdb_path"]
    db_conn_str = setup_sqlite["db_conn_str"]

    incoming_ocns_list = {
        "case_1_zephir_has_record_ocn": [33393343], 
        "case_2_zephir_does_not_have_record_ocn": [44192417],
    }

    expected_oclc_clusters = [[28477569, 33393343, 44192417]]
    inquiry_ocns_zephir = [28477569, 33393343, 44192417]
    expected_cid_ocn_list = [('009547317','28477569'), ('009547317', '33393343')]
    expected_zephir_clsuters = {
        "009547317": ['28477569', '33393343'],
    }

    for k, incoming_ocns in incoming_ocns_list.items():
        result = cid_inquiry(incoming_ocns, db_conn_str, primarydb_path, clusterdb_path)
        assert result["inquiry_ocns"] == incoming_ocns
        assert result["matched_oclc_clusters"] == expected_oclc_clusters
        assert result["num_of_matched_oclc_clusters"] == 1
        assert result["inquiry_ocns_zephir"] == inquiry_ocns_zephir
        assert result["cid_ocn_list"] == expected_cid_ocn_list
        assert result["cid_ocn_clusters"] == expected_zephir_clsuters
        assert result["num_of_matched_zephir_clusters"] == 1

def test_case_2_b_i(setup_leveldb, setup_sqlite):
    """ Test case 2.b.i.1):
        1. Incoming record contains 2+ OCNs that matches a single Concordance Table primary record
        b. Record OCN + Concordance OCN(s) matches one CID
        i. Concordance primary record has one OCN (equals to Record OCN)
        1). Matched Zephir cluster contains the OCN

        Test datasets:
        Zephir cluster: CID: 000249880; OCN: 999531
        OCLC primary OCN: 999531; other OCNs: None
        Incoming ocn: 999531, 12345678903
    """
    primarydb_path = setup_leveldb["primarydb_path"]
    clusterdb_path = setup_leveldb["clusterdb_path"]
    db_conn_str = setup_sqlite["db_conn_str"]

    incoming_ocns = [999531, 12345678903]
    expected_oclc_clusters = [[999531]]
    expected_cid_ocn_list = [('000249880', '999531')]
    expected_zephir_clsuters = {
        "000249880": ['999531'],
    }
    result = cid_inquiry(incoming_ocns, db_conn_str, primarydb_path, clusterdb_path)

    assert result["inquiry_ocns"] == incoming_ocns
    assert result["matched_oclc_clusters"] == expected_oclc_clusters
    assert result["num_of_matched_oclc_clusters"] == 1
    assert result["inquiry_ocns_zephir"] == incoming_ocns
    assert result["cid_ocn_list"] == expected_cid_ocn_list
    assert result["cid_ocn_clusters"] == expected_zephir_clsuters
    assert result["num_of_matched_zephir_clusters"] == 1

def test_case_2_b_ii_1_and_2(setup_leveldb, setup_sqlite):
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
          2) 44192417, 12345678904 - Zephir cluster doesn't have the Record OCN
    """
    primarydb_path = setup_leveldb["primarydb_path"]
    clusterdb_path = setup_leveldb["clusterdb_path"]
    db_conn_str = setup_sqlite["db_conn_str"]

    incoming_ocns_list = {
        "case_1_zephir_has_record_ocn": [33393343, 28477569], 
        "case_2_zephir_does_not_have_record_ocn": [44192417, 12345678904],
    }

    expected_oclc_clusters = [[28477569, 33393343, 44192417]]
    inquiry_ocns_zephir = {
        "case_1_zephir_has_record_ocn": [28477569, 33393343, 44192417],
        "case_2_zephir_does_not_have_record_ocn": [28477569, 33393343, 44192417, 12345678904],
    }
    expected_cid_ocn_list = [('009547317','28477569'), ('009547317', '33393343')]
    expected_zephir_clsuters = {
        "009547317": ['28477569', '33393343'],
    }
    expected_min_cid = "009547317"

    for k, incoming_ocns in incoming_ocns_list.items():
        result = cid_inquiry(incoming_ocns, db_conn_str, primarydb_path, clusterdb_path)
        assert result["inquiry_ocns"] == incoming_ocns
        assert result["matched_oclc_clusters"] == expected_oclc_clusters
        assert result["num_of_matched_oclc_clusters"] == 1
        assert result["inquiry_ocns_zephir"] == inquiry_ocns_zephir[k]
        assert result["cid_ocn_list"] == expected_cid_ocn_list
        assert result["cid_ocn_clusters"] == expected_zephir_clsuters
        assert result["num_of_matched_zephir_clusters"] == 1
        assert result["min_cid"] == expected_min_cid


## Test case c: incoming record matches 2+ CID
def test_case_1_and_2_c(setup_leveldb, setup_sqlite):
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
    primarydb_path = setup_leveldb["primarydb_path"]
    clusterdb_path = setup_leveldb["clusterdb_path"]
    db_conn_str = setup_sqlite["db_conn_str"]

    incoming_ocns = [217211158, 8727632] 

    expected_oclc_clusters = [[8727632, 24253253]]
    inquiry_ocns_zephir = [8727632, 24253253, 217211158] 

    expected_cid_ocn_list = [('000000280','217211158'), ('000000280','25909'), ('002492721','8727632')]
    expected_zephir_clsuters = {
        "000000280": ['217211158', '25909'],
        "002492721": ['8727632'],
    }
    expected_min_cid = "000000280"

    result = cid_inquiry(incoming_ocns, db_conn_str, primarydb_path, clusterdb_path)
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
def test_case_3_a(setup_leveldb, setup_sqlite):
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
    primarydb_path = setup_leveldb["primarydb_path"]
    clusterdb_path = setup_leveldb["clusterdb_path"]
    db_conn_str = setup_sqlite["db_conn_str"]

    incoming_ocns = [100, 300] 

    expected_oclc_clusters = [[300, 39867290, 39867383], [100]]
    inquiry_ocns_zephir = [100, 300, 39867290, 39867383] 

    expected_cid_ocn_list = []
    expected_zephir_clsuters = {}
    expected_min_cid = None

    result = cid_inquiry(incoming_ocns, db_conn_str, primarydb_path, clusterdb_path)
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
def test_case_3_b(setup_leveldb, setup_sqlite):
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
    primarydb_path = setup_leveldb["primarydb_path"]
    clusterdb_path = setup_leveldb["clusterdb_path"]
    db_conn_str = setup_sqlite["db_conn_str"]

    incoming_ocns = [200, 228676186]

    expected_oclc_clusters = [[4912741, 5066412, 23012053, 228676186, 315449541], [200, 1078101879, 1102728950]]
    inquiry_ocns_zephir = [200, 4912741, 5066412, 23012053, 228676186, 315449541, 1078101879, 1102728950]

    expected_cid_ocn_list = [('008648991', '23012053'), ('008648991', '4912741'), ('008648991', '5066412')]
    expected_zephir_clsuters = {"008648991": ['23012053', '4912741', '5066412']}
    expected_min_cid =  "008648991"

    result = cid_inquiry(incoming_ocns, db_conn_str, primarydb_path, clusterdb_path)
    print(result)

    assert result["inquiry_ocns"] == incoming_ocns
    assert result["matched_oclc_clusters"] == expected_oclc_clusters
    assert result["num_of_matched_oclc_clusters"] == 2
    assert result["inquiry_ocns_zephir"] == inquiry_ocns_zephir
    assert result["cid_ocn_list"] == expected_cid_ocn_list
    assert result["cid_ocn_clusters"] == expected_zephir_clsuters
    assert result["num_of_matched_zephir_clusters"] == 1 
    assert result["min_cid"] ==  expected_min_cid


def test_case_3_c(setup_leveldb, setup_sqlite):
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
    primarydb_path = setup_leveldb["primarydb_path"]
    clusterdb_path = setup_leveldb["clusterdb_path"]
    db_conn_str = setup_sqlite["db_conn_str"]

    incoming_ocns = [140869, 2094039]

    expected_oclc_clusters = [[2094039, 1008263420], [140869, 1150810243]]
    inquiry_ocns_zephir = [140869,  2094039,  1008263420, 1150810243]

    expected_cid_ocn_list = [
            ('000002076', '140869'), 
            ('000002076', '2094039'), 
            ('000002076', '241092814'),
            ('102337772', '1008263420')
            ]
    expected_zephir_clsuters = {
            "000002076": ['140869', '2094039', '241092814'],
            "102337772": ['1008263420']
            }
    expected_min_cid =  "000002076"

    result = cid_inquiry(incoming_ocns, db_conn_str, primarydb_path, clusterdb_path)
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
def test_case_4_abc(setup_leveldb, setup_sqlite):
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
    primarydb_path = setup_leveldb["primarydb_path"]
    clusterdb_path = setup_leveldb["clusterdb_path"]
    db_conn_str = setup_sqlite["db_conn_str"]

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
        "1_cid": [('102337775', '1234567890101')],
        "2_cids": [('102337774', '1234567890102'), ('102337776', '1234567890103')],
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
        result = cid_inquiry(incoming_ocns, db_conn_str, primarydb_path, clusterdb_path)
        assert result["inquiry_ocns"] == incoming_ocns
        assert result["matched_oclc_clusters"] == expected_oclc_clusters
        assert result["num_of_matched_oclc_clusters"] == 0 
        assert result["inquiry_ocns_zephir"] == inquiry_ocns_zephir[k]
        assert result["cid_ocn_list"] == expected_cid_ocn_list[k]
        assert result["cid_ocn_clusters"] == expected_zephir_clsuters[k]
        assert result["num_of_matched_zephir_clusters"] == expected_num_of_zephir_clsuters[k] 
        assert result["min_cid"] ==  expected_min_cid[k]

def test_flat_and_dedup_sort_list():
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


# FIXTURES
@pytest.fixture
def setup_leveldb(tmpdatadir, csv_to_df_loader):
    dfs = csv_to_df_loader
    primarydb_path = create_primary_db(tmpdatadir, dfs["primary.csv"])
    clusterdb_path = create_cluster_db(tmpdatadir, dfs["primary.csv"])
    return {
        "tmpdatadir": tmpdatadir,
        "dfs": dfs,
        "primarydb_path": primarydb_path,
        "clusterdb_path": clusterdb_path
    }

@pytest.fixture
def setup_sqlite(data_dir, tmpdir, scope="session"):
    db_name = "test_db_for_zephir.db"
    #database = os.path.join(tmpdir, db_name)
    database = os.path.join(data_dir, db_name)
    setup_sql = os.path.join(data_dir, "setup_zephir_test_db.sql")

    cmd = "sqlite3 {} < {}".format(database, setup_sql)
    print(cmd)
    os.system(cmd)

    return {
        "db_conn_str": 'sqlite:///{}'.format(database)
    }

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
