import os
import sys
import environs
import re

from sqlalchemy import create_engine
from sqlalchemy import text

from lib.utils import ConsoleMessenger
import lib.utils as utils

from oclc_lookup import lookup_ocns_from_oclc
from zephir_cluster_lookup import zephir_clusters_lookup

def get_config_by_key(config_dir_name, config_fname, key):
    """return config value by key from .yml config file
       config_dir: directory of configuration files
       config_fname: configuration filename
       key: configuration key
    """
    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, config_dir_name)

    # load all configuration files in directory
    configs = utils.load_config(CONFIG_PATH)
    print(configs)

    # get config value by filename and key
    config = configs.get(config_fname, {}).get(key)
    return config

def cid_inquiry(ocns, db_conn_str, primary_db_path, cluster_db_path):
    """Find Zephir clusters by given OCNs and their associated OCLC OCNs.
       1. Find associated OCLC OCNs
       2. Combine incoming OCNs and OCLC OCNs, remove duplicates  
       3. Find Zephir clusters by the combined OCNs
    Args:
        ocns: list of intergers representing OCNs
        db_conn_str: database connection string
        primary_db_path: full path to the OCNs primary LevelDB
        cluster_db_path: full path to the OCNs cluster LevelDB
    Returns: a dict combining both OCLC lookup and Zephir lookup results:
       "inquiry_ocns": input ocns, list of integers.
       "matched_oclc_clusters": OCNs in matched OCLC clusters, list of lists in integers.
       "num_of_matched_oclc_clusters": number of matched OCLC clusters.
       "inquiry_ocns_zephir": ocns list used to query Zephir DB.
       "cid_ocn_list": list of cid and ocn tuples from DB query.
       "cid_ocn_clusters": dict with key="cid", value=list of ocns in the cid cluster
       "num_of_matched_zephir_clusters": number of matched Zephir clusters.
    """

    # Lookups OCN clusters by a list of OCNs in integer
    oclc_lookup_result = lookup_ocns_from_oclc(ocns, primary_db_path, cluster_db_path)

    # combine incoming and OCLC ocns, and dedup
    oclc_ocns_list = oclc_lookup_result["matched_oclc_clusters"]
    combined_ocns_list = flat_and_dedup_sort_list([ocns] + oclc_ocns_list)

    # Finds Zephir clusters by list of OCNs and returns compiled results
    zephir_clusters_result = zephir_clusters_lookup(db_conn_str, combined_ocns_list)

    return {**oclc_lookup_result, **zephir_clusters_result}

def flat_and_dedup_sort_list(list_of_lists):
    new_list = []
    for a_list in list_of_lists:
        for item in a_list:
            if item not in new_list:
                new_list.append(item)
    return sorted(new_list)

def main():
    if (len(sys.argv) > 1):
        env = sys.argv[1]
    else:
        env = "test"

    # get environment variable in .env file
    ENV = os.environ.get("MINTER_ENV") or env
    print("env: {}".format(ENV))

    minter_db_config = get_config_by_key('config','minter_db', ENV)
    leveldb_config = get_config_by_key('config','ocns_leveldb', ENV)

    DB_CONNECT_STR = os.environ.get("OVERRIDE_DB_CONNECT_STR") or str(utils.db_connect_url(minter_db_config))
    print(DB_CONNECT_STR)

    PRIMARY_DB_PATH = os.environ.get("OVERRIDE_PRIMAR_DB_PATH") or leveldb_config["primary_db_path"]
    CLUSTER_DB_PATH = os.environ.get("OVERRIDE_CLUSTERDB_PATH") or leveldb_config["cluster_db_path"]

    print(PRIMARY_DB_PATH)
    print(CLUSTER_DB_PATH)

    #clusters = get_clusters_by_ocns(ocns)

    ocns_str = "'6758168','15437990','5663662','33393343','28477569','8727632'"
    ocns_list = [6758168, 15437990, 5663662, 33393343, 28477569, 8727632]

    results = find_zephir_clusters_by_ocns(DB_CONNECT_STR, ocns_str)
    print(results)

    zephir = ZephirClusterLookupResults(ocns_list, results)

    print(zephir.cid_ocn_list)
    print(zephir.cid_ocn_clusters)
    print(zephir.num_of_matched_clusters)
    print(zephir.inquiry_ocns)

if __name__ == '__main__':
    main()
