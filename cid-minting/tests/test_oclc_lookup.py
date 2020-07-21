import os

import msgpack
import pytest
import plyvel

from oclc_lookup import get_primary_ocn
from oclc_lookup import get_ocns_cluster_by_primary_ocn
from oclc_lookup import get_ocns_cluster_by_ocn
from oclc_lookup import get_clusters_by_ocns
from oclc_lookup import convert_set_to_list
from oclc_lookup import lookup_ocns_from_oclc

# TESTS
def test_get_primary_ocn(setup):
    input = list(setup["dfs"]["primary.csv"]["ocn"])
    expect = list(setup["dfs"]["primary.csv"]["primary"])
    result = [
        get_primary_ocn(ocn, setup["primarydb_path"])
        for ocn in input
    ]
    assert sorted(expect) == sorted(result)

def test_get_primary_ocn_with_null_cases(setup):
    # case: ocn passed is None
    result = get_primary_ocn(None, setup["primarydb_path"])
    assert result == None
    # case: ocn not in the database
    result = get_primary_ocn(0, setup["primarydb_path"])
    assert result == None

def test_get_ocns_cluster_by_primary_ocn(setup):
    primary_ocn = 1
    cluster = [9987701, 53095235, 433981287, 6567842]
    result = get_ocns_cluster_by_primary_ocn(primary_ocn, setup["clusterdb_path"])
    assert sorted(cluster) == sorted(result)

def test_get_cluster_missing_primary(setup):
    primary_ocn = 1
    result = get_ocns_cluster_by_primary_ocn(primary_ocn, setup["clusterdb_path"])
    assert primary_ocn not in result

def test_get_ocns_cluster_by_primary_ocn_2(setup):
    primary_ocn = 17216714 
    cluster = [535434196]
    result = get_ocns_cluster_by_primary_ocn(primary_ocn, setup["clusterdb_path"])
    assert sorted(cluster) == sorted(result)

def test_get_cluster_ocn_with_null_cases(setup):
    null_cases = {
        "cluster_of_one_ocn": 1000000000,
        "secondary_ocn": 6567842,
        "invalid_ocn": 1234567890,
        "none_ocn": None,
    }
    for k,v in null_cases.items():
        assert None == get_ocns_cluster_by_primary_ocn(v, setup["clusterdb_path"])
        
def test_get_ocns_cluster_by_ocn(setup):
    clusters = {
        # ocn: list of all ocns of the cluster
        1000000000: [1000000000],                               # cluster_of_one_ocn
        1: [6567842, 9987701, 53095235, 433981287, 1],          # cluster_of_multi_ocns_by_primary_ocn
        6567842: [1, 6567842, 9987701, 53095235, 433981287],    # cluster_of_multi_ocns_by_other_ocn
        17216714: [17216714, 535434196],                        # cluster_of_2_ocns_by_primary_ocn, 
    }
    for ocn, cluster in clusters.items():
        result = get_ocns_cluster_by_ocn(ocn, setup["primarydb_path"], setup["clusterdb_path"])
        assert sorted(cluster) == sorted(result)

def test_get_ocns_cluster_by_ocn_with_null_cases(setup):
    null_cases = {
        "invalid_ocn": 1234567890,
        "none_ocn": None,
    }
    for k, v in null_cases.items():
        assert None == get_ocns_cluster_by_ocn(v, setup["primarydb_path"], setup["clusterdb_path"])

def test_get_ocns_cluster_by_ocns(setup):
    clusters = {
        # primary_ocn, list of all ocns of the cluster 
        1000000000: [1000000000],                               # cluster_of_one_ocn
        1: [6567842, 9987701, 53095235, 433981287, 1],          # cluster_of_multi_ocns
        17216714: [17216714, 535434196],                        # cluster_of_2_ocns,
    }
    sets = {
        1000000000: {(1000000000,)},
        1: {(1, 6567842, 9987701, 53095235, 433981287)},
        17216714: {(17216714, 535434196)},
    }
    input_ocns_list = {
        "1_one_primary_ocn_cluster_of_one": [1000000000],
        "2_one_other_ocn_cluster_of_multi": [6567842],
        "3_two_primary_ocns_dups": [1000000000, 1000000000],
        "4_two_primary_ocns": [1, 1000000000],
        "5_ocns_with_primary_secondary_dups_invalid": [1, 1, 6567842, 17216714, 535434196, 12345678901, 1000000000],
    }
    expected_set = {
        "1_one_primary_ocn_cluster_of_one": sets[1000000000],
        "2_one_other_ocn_cluster_of_multi": sets[1],
        "3_two_primary_ocns_dups": sets[1000000000],
        "4_two_primary_ocns": (sets[1] | sets[1000000000]),
        "5_ocns_with_primary_secondary_dups_invalid": (sets[1] | sets[17216714] | sets[1000000000]),
    }
    
    for k, ocns in input_ocns_list.items():
        result = get_clusters_by_ocns(ocns, setup["primarydb_path"], setup["clusterdb_path"])
        print(result)
        assert result != None
        assert result == expected_set[k] 

def test_get_ocns_cluster_by_ocns_wthnull_cases(setup):
    input_ocns_list = {
        "one_invalid_ocn": [1234567890],
        "two_invalid_ocns": [1234567890, 12345678901],
        "no_ocns": [],
    }
    for k, ocns in input_ocns_list.items():
        result = get_clusters_by_ocns(ocns, setup["primarydb_path"], setup["clusterdb_path"])
        assert result == set()

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


def test_lookup_ocns_from_oclc(setup):
    input_ocns = {
        "one_ocn_primary_single_cluster": [1000000000],
        "one_ocn_primary_multi_cluster": [1],
        "one_other_ocn": [6567842],
        "two_ocns": [1000000000, 6567842],
        "one_invalid": [1234567890],
        "two_invalid": [1234567890, 12345678901],
    }

    expected = {
        "one_ocn_primary_single_cluster": {
            "inquiry_ocns": [1000000000],
            "matched_oclc_clusters": [[1000000000]],
            "num_of_matched_oclc_clusters": 1,
            },
        "one_ocn_primary_multi_cluster": {
            "inquiry_ocns": [1],
            "matched_oclc_clusters": [[1, 6567842, 9987701, 53095235, 433981287]],
            "num_of_matched_oclc_clusters": 1,
            },
        "one_other_ocn": {
            "inquiry_ocns": [6567842],
            "matched_oclc_clusters": [[1, 6567842, 9987701, 53095235, 433981287]],
            "num_of_matched_oclc_clusters": 1,
            },
        "two_ocns": {
            "inquiry_ocns": [1000000000, 6567842],
            "matched_oclc_clusters": [[1000000000], [1, 6567842, 9987701, 53095235, 433981287]],
            "num_of_matched_oclc_clusters": 2,
            },
        "one_invalid": {
            "inquiry_ocns": [1234567890],
            "matched_oclc_clusters": [],
            "num_of_matched_oclc_clusters": 0,
            },
        "two_invalid": {
            "inquiry_ocns": [1234567890, 12345678901],
            "matched_oclc_clusters": [],
            "num_of_matched_oclc_clusters": 0,
            },
    }

    for k, ocns in input_ocns.items():
        result = lookup_ocns_from_oclc(ocns, setup["primarydb_path"], setup["clusterdb_path"])
        assert result["inquiry_ocns"] == ocns
        assert result["matched_oclc_clusters"] == expected[k]["matched_oclc_clusters"]
        assert result["num_of_matched_oclc_clusters"] == expected[k]["num_of_matched_oclc_clusters"]


# FIXTURES
@pytest.fixture
def setup(tmpdatadir, csv_to_df_loader):
    dfs = csv_to_df_loader
    primarydb_path = create_primary_db(tmpdatadir, dfs["primary.csv"])
    clusterdb_path = create_cluster_db(tmpdatadir, dfs["primary.csv"])
    return {
        "tmpdatadir": tmpdatadir,
        "dfs": dfs,
        "primarydb_path": primarydb_path,
        "clusterdb_path": clusterdb_path
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
