import os
import sys
import environs
import re

from sqlalchemy import create_engine
from sqlalchemy import text

from lib.utils import ConsoleMessenger
import lib.utils as utils

from oclc_lookup import OclcLookupResult 
from oclc_lookup import get_clusters_by_ocns
from oclc_lookup import convert_set_to_list
from zephir_cluster_lookup import ZephirClusterLookupResults
from zephir_cluster_lookup import find_zephir_clusters_by_ocns

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
    """
    Args:
        ocns: list of intergers representing OCNs
        db_conn_str: database connection string
        primary_db_path: full path to the OCNs primary LevelDB
        cluster_db_path: full path to the OCNs cluster LevelDB

    Returns:

    """

    # oclc lookup by a list of OCNs
    # returns: A Set of tuples containing OCNs of resolved OCN clusters
    result_set_of_tuples = get_clusters_by_ocns(ocns, primary_db_path, cluster_db_path)

    # convert to a list of OCNs lists
    oclc_ocns_list = convert_set_to_list(result_set_of_tuples)

    oclc_lookup_result = OclcLookupResult(ocns, oclc_ocns_list)

    # TO-DO: combine record ocns and OCLC ocns
    combined_ocns = "" + oclc_lookup_result.matched_ocns

    # Zephir lookup by OCNs in comma separated, single quoted string
    # returns: list of cid and ocn returned from Zephir DB 
    cid_ocn_list = find_zephir_clusters_by_ocns(db_conn_str, combined_ocns)

    zephir_clusters_result = ZephirClusterLookupResults(cid_ocn_list, combined_ocns) 
    return {
            "oclc_ocns_list": oclc_ocns_list, 
            "oclc_lookup_result": oclc_lookup_result, 
            "cid_ocn_list": cid_ocn_list, 
            "zephir_clusters_result": zephir_clusters_result}


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

    results = find_zephir_clusters_by_ocns(DB_CONNECT_STR, ocns_str)
    print(results)

    zephir = ZephirClusterLookupResults(results, ocns_str)

    print(zephir.cid_ocn_list)
    print(zephir.cid_ocn_clusters)
    print(zephir.num_of_matched_clusters)
    print(zephir.inquiry_ocns)

if __name__ == '__main__':
    main()
