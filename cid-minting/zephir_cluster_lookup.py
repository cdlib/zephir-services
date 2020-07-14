import os
import sys
import environs
import re

from sqlalchemy import create_engine
from sqlalchemy import text

from lib.utils import ConsoleMessenger
import lib.utils as utils

SELECT_ZEPHIR_BY_OCLC = """SELECT distinct z.cid, i.identifier
    FROM zephir_records as z
    INNER JOIN zephir_identifier_records as r on r.record_autoid = z.autoid
    INNER JOIN zephir_identifiers as i on i.autoid = r.identifier_autoid
    WHERE i.type = 'oclc'
"""
AND_IDENTIFIER_IN = "AND i.identifier in"
ORDER_BY = "ORDER BY z.cid, z.id"

def construct_select_zephir_cluster_by_ocns(ocns):
    if invalid_sql_in_clause_str(ocns):
        return None

    return SELECT_ZEPHIR_BY_OCLC + " " + AND_IDENTIFIER_IN + " (" + ocns + ") " + ORDER_BY

def get_db_conn_string_from_config_by_key(config_dir_name, config_fname, key):
    """return database connection string from db_config.yml file
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
    print(config)

    return str(utils.db_connect_url(config))

class ZephirDatabase:
    def __init__(self, db_connect_str):
        self.engine = create_engine(db_connect_str)

    def findall(self, sql, params=None):
        with self.engine.connect() as connection:
            results = connection.execute(sql, params or ())
            return results.fetchall()

class ZephirClusterLookupResults:
    def __init__(self, db_conn_str, ocns_str):
        results_cid_ocn_list = find_zephir_clusters_by_ocns(db_conn_str, ocns_str)

        self.cid_ocn_clusters = formatting_cid_ocn_clusters(results_cid_ocn_list)
        self.num_of_matched_clusters = len(self.cid_ocn_clusters)
        self.inquiry_ocns = ocns_str

def find_zephir_clusters_by_ocns(db_conn_str, ocns_str):
    select_zephir = construct_select_zephir_cluster_by_ocns(ocns_str)
    if select_zephir:
        try:
            zephir = ZephirDatabase(db_conn_str)
            return zephir.findall(text(select_zephir))
        except:
            return None
    return None

def formatting_cid_ocn_clusters(cid_ocn_list):
    """
    Args:
        cid_ocn_list: list of cid and ocn tuples .
    Returns:
        Number of unique cids in the input list 
    """
    # key=cid, val=[ocn1, ocn2]
    cid_ocns_dict = {}

    if cid_ocn_list:
        for cid_ocn in cid_ocn_list:
            cid, ocn = cid_ocn
            if cid in cid_ocns_dict:
                cid_ocns_dict[cid].append(ocn)
            else:
                cid_ocns_dict[cid] = [ocn]

    return cid_ocns_dict

def valid_sql_in_clause_str(input_str):
    """Validates if input is comma separated, single quoted strings.

    Returns:
        True: valid
        False: Invalid

        For example:
        True: 
            "'1', '2345'"
            "'1'"
        False: 
            "1, 2345"
            "'abc', 'xyz'"
            ""
    """

    if not input_str:
        return False

    if re.search(r"^'(\d+)'(\s)*((\s)*(,)(\s)*('(\d+)'))*$", input_str):
        return True
    
    return False

def invalid_sql_in_clause_str(input_str):
    return not valid_sql_in_clause_str(input_str)


def main():
    if (len(sys.argv) > 1):
        env = sys.argv[1]
    else:
        env = "test"

    # get environment variable in .env file
    ENV = os.environ.get("MINTER_ENV") or env
    print("env: {}".format(ENV))

    DB_CONNECT_STR = os.environ.get("OVERRIDE_DB_CONNECT_STR") or get_db_conn_string_from_config_by_key('config','minter_db', ENV)

    print(DB_CONNECT_STR)

    ocns_str = "'6758168','15437990','5663662','33393343','28477569','8727632'"

    results = find_zephir_clusters_by_ocns(DB_CONNECT_STR, ocns_str)
    print(type(results))
    print(results)

    print(formatting_cid_ocn_clusters(results))

    zephir = ZephirClusterLookupResults(DB_CONNECT_STR, ocns_str)

    print(zephir.cid_ocn_clusters)
    print(zephir.num_of_matched_clusters)
    print(zephir.inquiry_ocns)

if __name__ == '__main__':
    main()
