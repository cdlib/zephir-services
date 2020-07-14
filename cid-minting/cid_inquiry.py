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
    ocn_clusters = get_clusters_by_ocns(ocns, primary_db_path, cluster_db_path)
    oclc_lookup_result = OclcLookupResult(ocns, ocn_clusters)

    # TO-DO: combine record ocns and OCLC ocns
    zephir_clusters = find_zephir_clusters_by_ocns(db_conn_str, oclc_lookup_result.matched_ocns)
    zephir_clusters_result = ZephirClusterLookupResults(zephir_clusters, oclc_lookup_result.matched_ocns) 
    return ocn_clusters, oclc_lookup_result, zephir_clusters, zephir_clusters_result


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
