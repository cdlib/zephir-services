import os
import sys

import environs
import json
import logging
import time

import lib.utils as utils
from config import get_configs_by_filename

from oclc_lookup import ConcordanceLookup, lookup_ocns_from_oclc
from zephir_cluster_lookup import ZephirDatabase, zephir_clusters_lookup

def cid_inquiry(ocns, db_conn_str, concordance_db_path, lookup=None, zephir_db=None, timings=None):
    """Find Zephir clusters by given OCNs and their associated OCLC OCNs.
       1. Find associated OCLC OCNs
       2. Combine incoming OCNs and OCLC OCNs, remove duplicates  
       3. Find Zephir clusters by the combined OCNs
    Args:
        ocns: list of intergers representing OCNs
        db_conn_str: database connection string
        concordance_db_path: path to the SQLite OCLC concordance database
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
    started = time.perf_counter()
    oclc_lookup_result = lookup_ocns_from_oclc(ocns, concordance_db_path, lookup, timings)
    if timings is not None:
        timings["concordance_seconds"] = time.perf_counter() - started

    # combine incoming OCNs with and matched OCLC ocns, and dedup
    oclc_ocns_list = oclc_lookup_result["matched_oclc_clusters"]
    combined_ocns_list = flat_and_dedup_sort_list([ocns] + oclc_ocns_list)

    # Finds Zephir clusters by list of OCNs and returns compiled results
    zephir_clusters_result = zephir_clusters_lookup(db_conn_str, combined_ocns_list, db=zephir_db, timings=timings)

    return {**oclc_lookup_result, **zephir_clusters_result}

def flat_and_dedup_sort_list(list_of_lists):
    return sorted({item for values in list_of_lists for item in values})

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
                 With --stdin: read comma-separated OCNs per line and write a
                 JSON response with result and timings per line until EOF.
                 When OCNs present: 
                   1. retrieves Zephir clusters by given OCNs;
                   2. return Zephir clusters in JSON string.
                 When OCNs is not present:
                   1. find OCNs from the next input file;
                   2. retrieves Zephir clusters by given OCNs;
                   3. write Zephir clusters in JSON string to output file;
                   4. repeat 1-3 indefinitely.
    """

    if (len(sys.argv) != 2 and len(sys.argv) != 3):
        print("Parameter error.")
        print("Usage: {} env[dev|stg|prd] [comma_separated_ocns|--stdin]".format(sys.argv[0]))
        print("{} dev".format(sys.argv[0]))
        print("{} dev 1,6567842,6758168,8727632".format(sys.argv[0]))
        exit(1)

    env = sys.argv[1]
    if env not in ["test", "dev", "stg", "prd"]:
        usage(sys.argv[0])
        exit(1)

    db_connect_str = os.environ.get("OVERRIDE_DB_CONNECT_STR")
    if not db_connect_str:
        zephir_db_config = get_configs_by_filename("config", "zephir_db")
        db_connect_str = str(utils.db_connect_url(zephir_db_config[env]))

    cid_minting_config = get_configs_by_filename("config", "cid_minting")
    concordance_db_path = cid_minting_config["concordance_db_path"]
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

    DB_CONNECT_STR = db_connect_str
    CONCORDANCE_DB_PATH = os.environ.get("OVERRIDE_CONCORDANCE_DB_PATH") or concordance_db_path

    # Reuse these resources for the complete worker lifetime. Concordance caches
    # assume a fixed snapshot; CID query results always come from Zephir.
    with ConcordanceLookup(CONCORDANCE_DB_PATH) as lookup:
        zephir_db = ZephirDatabase(DB_CONNECT_STR)
        try:
            def inquire(ocns, timings=None):
                return cid_inquiry(ocns, DB_CONNECT_STR, CONCORDANCE_DB_PATH,
                                   lookup, zephir_db, timings)

            if len(sys.argv) == 3:
                if sys.argv[2] == "--stdin":
                    # Blocking line protocol for persistent clients and benchmarks.
                    # Each request is comma-separated OCNs; each response is JSON.
                    for line in sys.stdin:
                        timings = {}
                        started = time.perf_counter()
                        results = inquire(convert_comma_separated_str_to_int_list(line.strip()), timings)
                        timings["total_seconds"] = time.perf_counter() - started
                        print(json.dumps({"result": results, "timings": timings}), flush=True)
                else:
                    results = inquire(convert_comma_separated_str_to_int_list(sys.argv[2]))
                    print(json.dumps(results))
                return

            # Preserve the prepare.pl file protocol until its caller is migrated.
            while True:
                processed = False
                for file in os.listdir(cid_inquiry_data_dir):
                    if not file.endswith(".txt"):
                        continue
                    output_filename = os.path.join(cid_inquiry_data_dir, file)
                    done_filename = os.path.join(cid_inquiry_done_dir, file + ".done")
                    ocns_list = convert_comma_separated_str_to_int_list(file[37:][:-4])
                    results = inquire(ocns_list)
                    with open(output_filename, 'w') as output_file:
                        output_file.write(json.dumps(results))
                    os.rename(output_filename, done_filename)
                    processed = True
                if not processed:
                    time.sleep(0.001)
        finally:
            zephir_db.close()


if __name__ == '__main__':
    main()
