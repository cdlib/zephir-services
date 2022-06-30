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
    print("Usage: {} env[dev|stg|prd] path_to_IDs_file".format(script_name))

def config_logger(logfile):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    log_format = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
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
        argv[1]: Server environemnt (Required). Can be dev, stg, or prd.
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

    localdb_config = get_configs_by_filename(CONFIG_PATH, "cid_minting")
    localdb_conn_str = str(db_connect_url(localdb_config[env]["minter_db"]))
    primary_db_path = localdb_config["primary_db_path"]
    cluster_db_path = localdb_config["cluster_db_path"]
    logfile = localdb_config["logpath"]

    config_logger(logfile)

    logging.info("Start " + os.path.basename(__file__))
    logging.info("Env: {}".format(env))

    ZEPHIRDB_CONN_STR = os.environ.get("OVERRIDE_ZEPHIRDB_CONN_STR") or zephirdb_conn_str
    LOCALDB_CONN_STR = os.environ.get("OVERRIDE_LOCALDB_CONN_STR") or localdb_conn_str
    PRIMARY_DB_PATH = os.environ.get("OVERRIDE_PRIMARY_DB_PATH") or primary_db_path
    CLUSTER_DB_PATH = os.environ.get("OVERRIDE_CLUSTER_DB_PATH") or cluster_db_path

    config = {
        "zephirdb_conn_str": ZEPHIRDB_CONN_STR,
        "localdb_conn_str": LOCALDB_CONN_STR,
        "leveldb_primary_path": PRIMARY_DB_PATH,
        "leveldb_cluster_path": CLUSTER_DB_PATH,
    }

    # sample IDs in JSON
    # {"ocns": "80274381,25231018", "contribsys_id": "hvd000012735", "previous_sysids": "", "htid": "hvd.hw5jdo"}
    with open(ids_file) as json_file:
        data = json.load(json_file)
        for ids in data:
            print(ids)
    
            cid_minter = CidMinter(config, ids)
            cid = cid_minter.mint_cid() 
            print(cid)


if __name__ == "__main__":
    main()
