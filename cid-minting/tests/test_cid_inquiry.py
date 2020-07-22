import os

import msgpack
import pytest
import plyvel

from cid_inquiry import cid_inquiry
from cid_inquiry import flat_and_dedup_sort_list

# TESTS
def test_case_1_a(setup_leveldb, setup_sqlite):
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


def test_case_1_b_i_1(setup_leveldb, setup_sqlite):
    """ Test case 1.b.i.1):
        1. Incoming record contains one OCN that matches a single Concordance Table primary record
        b. Record OCN + Concordance OCN(s) matches one CID
        i. Concordance primary record has one OCN (equals to Record OCN)
        1). Matched Zephir cluster with one OCN

        Test datasets:
        Zephir cluster: CID: 002492721; OCN: 8727632
        OCLC primary OCN: 8727632; other OCNs: None
        Incoming ocn: 8727632
    """
    primarydb_path = setup_leveldb["primarydb_path"]
    clusterdb_path = setup_leveldb["clusterdb_path"]
    db_conn_str = setup_sqlite["db_conn_str"]

    incoming_ocns = [8727632]
    expected_oclc_clusters = [[8727632]]
    expected_cid_ocn_list = [('002492721', '8727632')]
    expected_zephir_clsuters = {
        "002492721": ['8727632'],
    }
    result = cid_inquiry(incoming_ocns, db_conn_str, primarydb_path, clusterdb_path)

    #  "iquiry_ocns": input ocns, list of integers.
    #  "matched_oclc_clusters": OCNs in matched OCLC clusters, list of lists in integers
    #  "num_of_matched_oclc_clusters": number of matched OCLC clusters#
    #  "inquiry_ocns_zephir": ocns used to quesry Zephir clusters
    #  "cid_ocn_list": list of cid and ocn tuples from DB query
    #  "cid_ocn_clusters": dict with key="cid", value=list of ocns in the cid cluster,
    #  "num_of_matched_zephir_clusters": num of matched Zephir clusters

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
          1) Zephir cluster contains the Record OCN - 33393343
          2) Zephir cluster doesn't have the Record OCN - 44192417
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

def test_case_2_b(setup_leveldb, setup_sqlite):
    """Test case 2.b:
    2. Incoming record contains 2+ OCNs that resolve to a single master record in the Concordance Table.
    b. Record OCNs + Concordance OCN(s) match one CID

    Test datasets:
    Zephir cluster:
    CID: 009547317; OCNs: 33393343, 28477569

    OCLC Primary OCN: 33393343
    Others OCNs: 28477569, 44192417

    Incoming OCNs:
      1) primary + other: [33393343, 28477569]
      2) two others: [44192417, 28477569]
      3) primary + invalid: [33393343, 1234567890] 
    """
    primarydb_path = setup_leveldb["primarydb_path"]
    clusterdb_path = setup_leveldb["clusterdb_path"]
    db_conn_str = setup_sqlite["db_conn_str"]

    expected_oclc_clusters = [[28477569, 33393343, 44192417]]
    expected_cid_ocn_list = [('009547317','28477569'), ('009547317', '33393343')]
    expected_zephir_clsuters = {
        "009547317": ['28477569', '33393343'],
    }

    incoming_ocns_list = {
        "1_primary_and_other_ocn": [33393343, 28477569],
        "2_two_other_ocns": [44192417, 28477569],
        "3_primary_and_invalid": [33393343, 1234567890],
    }

    inquiry_ocns_zephir = {
        "1_primary_and_other_ocn": [28477569, 33393343, 44192417],
        "2_two_other_ocns": [28477569, 33393343, 44192417],
        "3_primary_and_invalid": [28477569, 33393343, 44192417, 1234567890], 
    }

    for k, incoming_ocns in incoming_ocns_list.items():
        result = cid_inquiry(incoming_ocns, db_conn_str, primarydb_path, clusterdb_path)
        assert result["inquiry_ocns"] == incoming_ocns
        assert result["matched_oclc_clusters"] == expected_oclc_clusters
        assert result["num_of_matched_oclc_clusters"] == 1
        assert result["inquiry_ocns_zephir"] == inquiry_ocns_zephir[k]
        assert result["cid_ocn_list"] == expected_cid_ocn_list
        assert result["cid_ocn_clusters"] == expected_zephir_clsuters
        assert result["num_of_matched_zephir_clusters"] == 1

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
