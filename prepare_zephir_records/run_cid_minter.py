import os
import sys

import environs
import json
import logging
import time

from lib.utils import db_connect_url
from lib.utils import get_configs_by_filename

from cid_minter.oclc_lookup import lookup_ocns_from_oclc
from cid_minter.cid_inquiry_by_ocns import cid_inquiry_by_ocns
from cid_minter.cid_minter import CidMinter 

def usage(script_name):
    print("Parameter error.")
    print("Usage: {} env[test|dev|stg|prd] path_to_IDs_file".format(script_name))

def config_logger(logfile):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    log_format = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s: %(message)s')
    # output to file
    file = logging.FileHandler(logfile)
    file.setFormatter(log_format)

    # output to console
    stream = logging.StreamHandler()

    logger.addHandler(file)
    logger.addHandler(stream)

def main():
    """ Retrieves Zephir clusters by OCNs.
        Command line arguments:
        argv[1]: Server environemnt (Required). Can be test, dev, stg, or prd.
        argv[2]: path to a JSON IDs file
    """

    if (len(sys.argv) != 3):
        usage(sys.argv[0])
        exit(1)

    env = sys.argv[1]
    if env not in ["test", "dev", "stg", "prd"]:
        usage(sys.argv[0])
        exit(1)

    ids_file = sys.argv[2]

    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, "config")

    zephirdb_config = get_configs_by_filename(CONFIG_PATH, "zephir_db")
    zephirdb_conn_str = str(db_connect_url(zephirdb_config[env]))
    local_minterdb_conn_str = zephirdb_conn_str

    cid_minting_config = get_configs_by_filename(CONFIG_PATH, "cid_minting")

    primary_db_path = cid_minting_config["primary_db_path"]
    cluster_db_path = cid_minting_config["cluster_db_path"]
    logfile = cid_minting_config["logpath"]

    config_logger(logfile)

    logging.info("Start " + os.path.basename(__file__))
    logging.info("Env: {}".format(env))

    config = {
        "zephirdb_conn_str": zephirdb_conn_str,
        "local_minterdb_conn_str": local_minterdb_conn_str,
        "leveldb_primary_path": primary_db_path,
        "leveldb_cluster_path": cluster_db_path,
    }

    cid_minter = CidMinter(config)

    # sample IDs in JSON
    # {"ocns": "80274381,25231018", "contribsys_id": "hvd000012735", "previous_sysids": "", "htid": "hvd.hw5jdo"}
    with open(ids_file) as json_file:
        data = json.load(json_file)
        for ids in data:
            try:
                print(f"IDs: {ids}")
                cid = cid_minter.mint_cid(ids) 
                print(f"Minted CID {cid}")
            except Exception as e:
                print(f"Exception: {e}")
                continue


if __name__ == "__main__":
    main()
