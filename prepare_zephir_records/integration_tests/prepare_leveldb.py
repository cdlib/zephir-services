import os

import msgpack
import plyvel
import pandas as pd
import shutil

def create_leveldb(leveldb_dir, data_file):
    df = pd.read_csv(data_file)
    primary_db_path = create_primary_db(leveldb_dir, df)
    cluster_db_path = create_cluster_db(leveldb_dir, df)

    return {
        "leveldb_dir": leveldb_dir,
        "dfs": df,
        "primary_db_path": primary_db_path,
        "cluster_db_path": cluster_db_path
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
    shutil.rmtree(db_path, ignore_errors=True)
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
    shutil.rmtree(db_path, ignore_errors=True)
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

def main():
    leveldb_dir = "leveldb/"
    data_file = "test_datasets/leveldb_test_datasets.csv"
    
    shutil.rmtree(leveldb_dir, ignore_errors=True)
    os.mkdir(leveldb_dir)

    ret = create_leveldb(leveldb_dir, data_file)
    print("Creating levelDB: Complete")
    print(f"  leveldb_dir: {ret.get('leveldb_dir')}")
    print(f"  primary_db_path: {ret.get('primary_db_path')}")
    print(f"  cluster_db_path: {ret.get('cluster_db_path')}")

if __name__ == "__main__":
    main()
