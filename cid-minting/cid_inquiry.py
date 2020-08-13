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
       "min_cid": lowest CID among matched Zephir clusters
    """

    # Lookups OCN clusters by a list of OCNs in integer
    oclc_lookup_result = lookup_ocns_from_oclc(ocns, primary_db_path, cluster_db_path)

    # combine incoming OCNs with and matched OCLC ocns, and dedup
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
    """ Retrieves Zephir clusters by OCNs.
        Command line arguments:
        argv[1]: Server environemnt (Required). Can be dev, stg, or prd.
        argv[2]: List of OCNs (Required).
                 Comma separated strings without spaces in between any two values.
                 For example: 1,6567842,6758168,8727632
    """
    if (len(sys.argv) != 3):
        print("Parmeter error.")
        print("Usage: {} env[dev|stg|prd] comma_separated_ocns".format(sys.argv[0]))
        print("{} dev 1,6567842,6758168,8727632".format(sys.argv[0]))
        exit(1)

    env = sys.argv[1]
    ocns = sys.argv[2].split(",")

    ocns_list = [int(i) for i in ocns]

    zephir_db_config = get_config_by_key('config', 'zephir_db', env)
    db_connect_url = str(utils.db_connect_url(zephir_db_config))

    primary_db_path = get_config_by_key('config', 'ocns_leveldb', "primary_db_path")
    cluster_db_path = get_config_by_key('config', 'ocns_leveldb', "cluster_db_path")

    DB_CONNECT_STR = os.environ.get("OVERRIDE_DB_CONNECT_STR") or db_connect_url
    PRIMARY_DB_PATH = os.environ.get("OVERRIDE_PRIMARY_DB_PATH") or primary_db_path
    CLUSTER_DB_PATH = os.environ.get("OVERRIDE_CLUSTER_DB_PATH") or cluster_db_path

    results = cid_inquiry(ocns_list, DB_CONNECT_STR, PRIMARY_DB_PATH, CLUSTER_DB_PATH)
    print(results)
    exit(0)

if __name__ == '__main__':
    main()
