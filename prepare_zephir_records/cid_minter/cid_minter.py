import os
import sys

import environs
import json
import logging
import time

from lib.utils import db_connect_url
from lib.utils import get_configs_by_filename

from oclc_lookup import lookup_ocns_from_oclc
from zephir_cluster_lookup import ZephirDatabase
from cid_inquiry_by_ocns import cid_inquiry_by_ocns

class CidMinter:
    def __init__(self, config, ids):
        self.config = config
        self.ids = ids 

    def mint_cid(self):
        ocns = self.ids.get("ocns")
        zephirDb = ZephirDatabase(self.config.get("zephirdb_connect_str"))
        leveldb_primary_path = self.config.get("leveldb_primary_path")
        leveldb_cluster_path = self.config.get("leveldb_cluster_path")

        results = cid_inquiry_by_ocns(ocns, zephirDb, leveldb_primary_path, leveldb_cluster_path) 
        print(results)
        return results['min_cid']

def convert_comma_separated_str_to_int_list(ocn_str):
    int_list=[]
    str_list = ocn_str.split(",")
    for a_str in str_list:
        try:
            ocn = int(a_str)
        except ValueError:
            logging.error("ValueError: {}".format(a_str))
            continue
        if (ocn > 0):
            int_list.append(ocn)
        else:
            logging.error("ValueError: {}".format(a_str))

    return int_list

def usage(script_name):
    print("Parameter error.")
    print("Usage: {} env[dev|stg|prd] comma_separated_ocns".format(script_name))
    print("{} dev 1,6567842,6758168,8727632".format(script_name))

def main():
    """ Retrieves Zephir clusters by OCNs.
        Command line arguments:
        argv[1]: Server environemnt (Required). Can be dev, stg, or prd.
        argv[2]: List of OCNs (Optional).
                 Comma separated strings without spaces in between any two values.
                 For example: 1,6567842,6758168,8727632
                 When OCNs present: 
                   1. retrieves Zephir clusters by given OCNs;
                   2. return Zephir clusters in JSON string.
    """

    if (len(sys.argv) != 3):
        usage(sys.argv[0])
        exit(1)

    env = sys.argv[1]
    if env not in ["test", "dev", "stg", "prd"]:
        usage(sys.argv[0])
        exit(1)

    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, 'config')

    zephir_db_config = get_configs_by_filename(CONFIG_PATH, "zephir_db")
    db_connect_str = str(db_connect_url(zephir_db_config[env]))

    cid_minting_config = get_configs_by_filename("config", "cid_minting")
    primary_db_path = cid_minting_config["primary_db_path"]
    cluster_db_path = cid_minting_config["cluster_db_path"]
    logfile = cid_minting_config['logpath']
    cid_inquiry_data_dir = cid_minting_config['cid_inquiry_data_dir']
    cid_inquiry_done_dir = cid_minting_config['cid_inquiry_done_dir']

    logging.basicConfig(
            level=logging.DEBUG,
            filename=logfile,
            format="%(asctime)s %(levelname)-4s %(message)s",
        )
    logging.info("Start " + os.path.basename(__file__))
    logging.info("Env: {}".format(env))

    DB_CONNECT_STR = os.environ.get("OVERRIDE_DB_CONNECT_STR") or db_connect_str
    PRIMARY_DB_PATH = os.environ.get("OVERRIDE_PRIMARY_DB_PATH") or primary_db_path
    CLUSTER_DB_PATH = os.environ.get("OVERRIDE_CLUSTER_DB_PATH") or cluster_db_path

    config = {
        "zephirdb_connect_str": DB_CONNECT_STR,
        "leveldb_primary_path": PRIMARY_DB_PATH,
        "leveldb_cluster_path": CLUSTER_DB_PATH,
    }

    zephirDb = ZephirDatabase(DB_CONNECT_STR)

    if (len(sys.argv) == 3):
        ocns_list = convert_comma_separated_str_to_int_list(sys.argv[2])
        print("output from CidMinter")
        ids = {
            "ocns": ocns_list,
        }
        cid_minter = CidMinter(config, ids)
        cid = cid_minter.mint_cid() 
        print(cid)
        exit(0)


if __name__ == '__main__':
    main()
