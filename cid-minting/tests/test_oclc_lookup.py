import os

import msgpack
import pytest
import plyvel

from oclc_lookup import get_primary_ocn
from oclc_lookup import get_ocns_cluster_by_primary_ocn

# TESTS
def test_get_primary_ocn(setup):
    input = list(setup["dfs"]["primary.csv"]["ocn"])
    expect = list(setup["dfs"]["primary.csv"]["primary"])
    result = [
        get_primary_ocn(ocn, setup["primarydb_path"])
        for ocn in input
    ]
    assert expect.sort() == result.sort()

def test_get_primary_ocn_with_null_cases(setup):
    # case: ocn passed is None
    result = get_primary_ocn(None, setup["primarydb_path"])
    assert result == None
    # case: ocn not in the database
    result = get_primary_ocn(0, setup["primarydb_path"])
    assert result == None

def test_get_ocns_cluster_by_primary_ocn(setup):
    primary_ocn = 1
    cluster = [6567842, 9987701, 53095235, 433981287]
    result = get_ocns_cluster_by_primary_ocn(primary_ocn, setup["clusterdb_path"])
    assert cluster.sort() == result.sort()

def test_get_cluster_missing_primary(setup):
    primary_ocn = 1
    result = get_ocns_cluster_by_primary_ocn(primary_ocn, setup["clusterdb_path"])
    assert primary_ocn not in result

def test_get_cluster_ocn_with_null_cases(setup):
    null_cases = {
        "cluster_of_one_ocn": 1000000000,
        "secondary_ocn": 6567842,
        "invalid_ocn": 1234567890,
        "none_ocn": None,
    }
    for k,v in null_cases.items():
        assert None == get_ocns_cluster_by_primary_ocn(v, setup["clusterdb_path"])
        

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
