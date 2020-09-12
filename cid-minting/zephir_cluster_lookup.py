import os
import sys
import environs
import re

from sqlalchemy import create_engine
from sqlalchemy import text

import lib.utils as utils
from config import get_configs_by_filename

SELECT_ZEPHIR_BY_OCLC = """SELECT distinct z.cid cid, i.identifier ocn
    FROM zephir_records as z
    INNER JOIN zephir_identifier_records as r on r.record_autoid = z.autoid
    INNER JOIN zephir_identifiers as i on i.autoid = r.identifier_autoid
    WHERE z.cid != '0' AND i.type = 'oclc'
"""
AND_IDENTIFIER_IN = "AND i.identifier in"
AND_CID_IN = "AND z.cid in"
ORDER_BY = "ORDER BY z.cid, i.identifier"

def construct_select_zephir_cluster_by_ocns(ocns):
    if invalid_sql_in_clause_str(ocns):
        return None

    return SELECT_ZEPHIR_BY_OCLC + " " + AND_IDENTIFIER_IN + " (" + ocns + ") " + ORDER_BY

def construct_select_zephir_cluster_by_cid(cids):
    if invalid_sql_in_clause_str(cids):
        return None

    return SELECT_ZEPHIR_BY_OCLC + " " + AND_CID_IN + " (" + cids + ") " + ORDER_BY

class ZephirDatabase:
    def __init__(self, db_connect_str):
        self.engine = create_engine(db_connect_str)

    def findall(self, sql, params=None):
        with self.engine.connect() as connection:
            results = connection.execute(sql, params or ())
            results_dict = [dict(row) for row in results.fetchall()]
            return results_dict

def zephir_clusters_lookup(db_conn_str, ocns_list):
    """
    Finds Zephir clusters by OCNs and returns clusters' info including cluster IDs, number of clusters and all OCNs in each cluster. 
    Args:
        db_conn_str: database connection string
        ocns_list: list of OCNs in integers
    Return: A dict with:
        "inquiry_ocns_zephir": input ocns list,
        "cid_ocn_list": list of dict with keys of "cid" and "ocn",
        "cid_ocn_clusters": dict with key="cid", value=list of ocns in the cid cluster,
        "num_of_matched_zephir_clusters": number of matched clusters
        "min_cid": lowest CID 
    """
    zephir_cluster = {
        "inquiry_ocns_zephir": ocns_list,
        "cid_ocn_list": [],
        "cid_ocn_clusters": {},
        "num_of_matched_zephir_clusters": 0,
        "min_cid": None,
    }

    cid_ocn_list_by_ocns = find_zephir_clusters_by_ocns(db_conn_str, ocns_list)
    if not cid_ocn_list_by_ocns:
        return zephir_cluster

    # find all OCNs in each cluster
    cids_list = [cid_ocn.get("cid") for cid_ocn in cid_ocn_list_by_ocns]
    unique_cids_list = list(set(cids_list))
    cid_ocn_list = find_zephir_clusters_by_cids(db_conn_str, unique_cids_list)
    if not cid_ocn_list:
        return zephir_cluster

    # convert to a dict with key=cid, value=list of ocns
    cid_ocn_clusters = formatting_cid_ocn_clusters(cid_ocn_list)

    zephir_cluster = {
        "inquiry_ocns_zephir": ocns_list,
        "cid_ocn_list": cid_ocn_list,
        "cid_ocn_clusters": cid_ocn_clusters,
        "num_of_matched_zephir_clusters": len(cid_ocn_clusters),
        "min_cid": min([cid_ocn.get("cid") for cid_ocn in cid_ocn_list])
    }
    return zephir_cluster

def find_zephir_clusters_by_ocns(db_conn_str, ocns_list):
    """
    Args:
        db_conn_str: database connection string
        ocns_list: list of OCNs in integer 
    Returns:
        list of dict with keys "cid" and "ocn"
        [] when there is no match
        None: when there is an exception
    """
    select_zephir = construct_select_zephir_cluster_by_ocns(list_to_str(ocns_list))
    if select_zephir:
        try:
            zephir = ZephirDatabase(db_conn_str)
            return zephir.findall(text(select_zephir))
        except:
            return None
    return None

def find_zephir_clusters_by_cids(db_conn_str, cid_list):
    """
    Args:
        db_conn_str: database connection string
        cid_list: list of CIDs in string
    Returns:
        list of dict with keys "cid" and "ocn"
    """
    select_zephir = construct_select_zephir_cluster_by_cid(list_to_str(cid_list))
    if select_zephir:
        try:
            zephir = ZephirDatabase(db_conn_str)
            return zephir.findall(text(select_zephir))
        except:
            return None
    return None

def list_to_str(a_list):
    """Convert list item to a single quoted string, concat with a comma and space 
    """
    ocns = ""
    for item in a_list:
        if ocns:
            ocns += ", '" + str(item) + "'"
        else:
            ocns = "'" + str(item) + "'"
    return ocns

def formatting_cid_ocn_clusters(cid_ocn_list):
    """Put cid and ocn pairs into clusters by unique cids. 
    Args:
        cid_ocn_list: list of dict with keys of "cid" and "ocn".
        [{"cid": cid1, "ocn": ocn1}, {"cid": cid1, "ocn": ocn2}, {"cid": cid3, "ocn": ocn3}]
    Returns:
        A dict with key=unique cid, value=list of ocns with the same cid.
        {"cid1": [ocn1, ocn2],
         "cid3", [ocn3]}
    """
    # key: cid, value: list of ocns [ocn1, ocn2]
    cid_ocns_dict = {}

    if cid_ocn_list:
        for cid_ocn in cid_ocn_list:
            cid = cid_ocn.get("cid")
            ocn = cid_ocn.get("ocn")
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

    configs= get_configs_by_filename('config', 'zephir_db')
    print(configs)

    db_connect_str = str(utils.db_connect_url(configs[env]))

    DB_CONNECT_STR = os.environ.get("OVERRIDE_DB_CONNECT_STR") or db_connect_str

    #print(DB_CONNECT_STR)

    ocns_list = [6758168, 15437990, 5663662, 33393343, 28477569, 8727632]

    results = zephir_clusters_lookup(DB_CONNECT_STR, ocns_list)
    print(results)

if __name__ == '__main__':
    main()
