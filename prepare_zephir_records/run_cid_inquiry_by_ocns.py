import os
import sys

import environs
import json
import logging
import time

from lib.utils import db_connect_url
from lib.utils import get_configs_by_filename

from cid_minter.oclc_lookup import lookup_ocns_from_oclc
from cid_minter.zephir_cluster_lookup import ZephirDatabase
from cid_minter.cid_inquiry_by_ocns import cid_inquiry_by_ocns
from cid_minter.cid_inquiry_by_ocns import convert_comma_separated_str_to_int_list

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
                 When OCNs is not present:
                   1. find OCNs from the next input file;
                   2. retrieves Zephir clusters by given OCNs;
                   3. write Zephir clusters in JSON string to output file;
                   4. repeat 1-3 indefinitely or when there are no input files for 10 minutes.
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

    cid_minting_config = get_configs_by_filename(CONFIG_PATH, "cid_minting")
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

    zephirDb = ZephirDatabase(DB_CONNECT_STR)

    if (len(sys.argv) == 3):
        ocns_list = convert_comma_separated_str_to_int_list(sys.argv[2])

        results = cid_inquiry_by_ocns(ocns_list, zephirDb, PRIMARY_DB_PATH, CLUSTER_DB_PATH)
        print(json.dumps(results))

        exit(0)


if __name__ == '__main__':
    main()
