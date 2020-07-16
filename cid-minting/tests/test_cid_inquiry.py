import os

import msgpack
import pytest
import plyvel

from cid_inquiry import cid_inquiry
from cid_inquiry import convert_set_to_list
from cid_inquiry import flat_and_dedup_sort_list

# TESTS
def test_case_1_b_i_1(setup_leveldb, setup_sqlite):
    """ Test case 1.b.i.1):
        1. Incoming record contains one OCN that matches a single Concordance Table primary record
        b. Record OCN + Concordance OCN(s) matches one CID
        i. Concordance primary record has one OCN (equals to Record OCN)
        1). Matched Zephir cluster with one OCN

        Test datasets:
        Zephir cluster: CID: 002492721; OCN: 8727632
        OCLC primary OCN: 8727632; other OCNs: None
    """
    primarydb_path = setup_leveldb["primarydb_path"]
    clusterdb_path = setup_leveldb["clusterdb_path"]
    db_conn_str = setup_sqlite["db_conn_str"]

    incoming_ocns = [8727632]
    expected_zephir_clsuter = {
        "002492721": ['8727632'],
    }
    results = cid_inquiry(incoming_ocns, db_conn_str, primarydb_path, clusterdb_path)

    print(results["oclc_ocns_list"])
    print(results["oclc_lookup_result"])
    print(results["cid_ocn_list"])
    print(results["zephir_clusters_result"])

    zephir_cluster_result = results["zephir_clusters_result"]
    assert zephir_cluster_result.cid_ocn_clusters == expected_zephir_clsuter
    assert zephir_cluster_result.num_of_matched_clusters == 1
    assert zephir_cluster_result.inquiry_ocns == incoming_ocns

def case_1_b_ii_1(create_test_db):
    """ Test case 1.b.i.1):
        1. Incoming record contains one OCN that matches a single Concordance Table primary record
        b. Record OCN + Concordance OCN(s) matches one CID
        ii. Concordance primary record has more than one OCNs
        1). Zephir cluster contains the Record OCN

        Test datasets:
        Zephir cluster:
        CID: 009547317; OCNs: 33393343, 28477569

        OCLC Primary OCN: 33393343
        Others OCNs: 28477569, 44192417
    """
    db_conn_str = os.environ.get("OVERRIDE_DB_CONNECT_STR")
    inquiry_ocns = "'33393343', '28477569', '44192417'"
    expected_zephir_clsuter = {
        "009547317": ['28477569', '33393343'],
    }
    cid_ocn_list = find_zephir_clusters_by_ocns(db_conn_str, inquiry_ocns)
    results = ZephirClusterLookupResults(cid_ocn_list, inquiry_ocns)
    print(results.cid_ocn_clusters)
    print(results.num_of_matched_clusters)
    print(results.inquiry_ocns)
    assert results.cid_ocn_clusters == expected_zephir_clsuter
    assert results.num_of_matched_clusters == 1
    assert results.inquiry_ocns == inquiry_ocns

def test_convert_set_to_list():
    input_sets = {
        "one_tuple_single_item": {(1000000000,)},
        "one_tuple_multi_items": {(1, 6567842, 9987701, 53095235, 433981287)},
        "two_tuples": {(1000000000,), (1, 6567842, 9987701, 53095235, 433981287)},
        "empty_set": set(),
    }
    expected_lists = {
        "one_tuple_single_item": [[1000000000]],
        "one_tuple_multi_items": [[1, 6567842, 9987701, 53095235, 433981287]],
        "two_tuples": [[1000000000], [1, 6567842, 9987701, 53095235, 433981287]],
        "empty_set": []
    }
    for k, a_set in input_sets.items():
        assert convert_set_to_list(a_set) == expected_lists[k]

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
