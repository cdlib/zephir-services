import os
import sys

import environs
import json
import logging
import time

from prepare_zephir_records.lib.utils import db_connect_url
from prepare_zephir_records.lib.utils import get_configs_by_filename

from prepare_zephir_records.cid_minter.oclc_lookup import lookup_ocns_from_oclc
from prepare_zephir_records.cid_minter.zephir_cluster_lookup import ZephirDatabase

def cid_inquiry_by_ocns(ocns, zephirDb, primary_db_path, cluster_db_path):
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
    zephir_clusters_result = zephirDb.zephir_clusters_lookup(combined_ocns_list)

    return {**oclc_lookup_result, **zephir_clusters_result}

def flat_and_dedup_sort_list(list_of_lists):
    new_list = []
    for a_list in list_of_lists:
        for item in a_list:
            if item not in new_list:
                new_list.append(item)
    return sorted(new_list)

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
    ROOT_PATH = os.path.dirname(ROOT_PATH)
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