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

def construct_select_zephir_cluster_by_contribsys_id(contribsys_ids):
    if contribsys_ids:
        return "SELECT distinct cid, contribsys_id FROM zephir_records WHERE contribsys_id in (" + contribsys_ids + ") order by cid"
    else:
        return None

def construct_select_zephir_cluster_by_cid_and_contribsys_id(cid, contribsys_ids):
    if cid and contribsys_ids:
        return "SELECT distinct cid, contribsys_id FROM zephir_records WHERE cid='" + cid + "' and contribsys_id not in (" + contribsys_ids + ") order by cid"
    else:
        return None

class ZephirDatabase:
    def __init__(self, db_connect_str):
        self.engine = create_engine(db_connect_str)

    def findall(self, sql, params=None):
        with self.engine.connect() as conn:
            try:
                results = conn.execute(sql, params or ())
                results_dict = [dict(row) for row in results.fetchall()]
                return results_dict
            except SQLAlchemyError as e:
                print("DB error: {}".format(e))
                return None
            

    def insert(self, db_table, records):
        """insert multiple records to a db table
           Args:
               db_table: table name in string
               records: list of records in dictionary
            Returns: None
               Idealy the number of affected rows. However sqlalchemy does not support this feature.
               The CursorResult.rowcount suppose to return the number of rows matched, 
               which is not necessarily the same as the number of rows that were actually modified.
               However, the result.rowcount here always returns -1.
        """ 
        with self.engine.connect() as conn:
            for record in records:
                try:
                    insert_stmt = insert(db_table).values(record)
                    result = conn.execute(insert_stmt)
                except SQLAlchemyError as e:
                    print("DB insert error: {}".format(e))

    def insert_update_on_duplicate_key(self, db_table, records):
        """insert multiple records to a db table
           insert when record is new
           update on duplicate key - update only when the content is changed 
           Args:
               db_table: table name in string
               records: list of records in dictionary
            Returns: None
               Idealy the number of affected rows. However sqlalchemy does not support this feature.
               The CursorResult.rowcount suppose to return the number of rows matched, 
               which is not necessarily the same as the number of rows that were actually modified.
               However, the result.rowcount here always returns -1.
        """
        with self.engine.connect() as conn:
            for record in records:
                try:
                    insert_stmt = insert(db_table).values(record)
                    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(record)
                    result = conn.execute(on_duplicate_key_stmt)
                except SQLAlchemyError as e:
                    print("DB insert error: {}".format(e))

    def close(self):
        self.engine.dispose()

def zephir_clusters_lookup(zephirDb, ocns_list):
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

    cid_ocn_list_by_ocns = find_zephir_clusters_by_ocns(zephirDb, ocns_list)
    if not cid_ocn_list_by_ocns:
        return zephir_cluster

    # find all OCNs in each cluster
    cids_list = [cid_ocn.get("cid") for cid_ocn in cid_ocn_list_by_ocns]
    unique_cids_list = list(set(cids_list))
    cid_ocn_list = find_zephir_clusters_by_cids(zephirDb, unique_cids_list)
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

def find_zephir_clusters_by_ocns(zephirDb, ocns_list):
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
            results = zephirDb.findall(text(select_zephir))
            return results
        except:
            return None
    return None

def find_zephir_clusters_by_cids(zephirDb, cid_list):
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
            results = zephirDb.findall(text(select_zephir))
            return results 
        except:
            return None
    return None

def find_zephir_clusters_by_contribsys_ids(zephirDb, contribsys_id_list):
    """
    Args:
        db_conn_str: database connection string
        contribsys_id_list: list of contribsys IDs in string
    Returns:
        list of dict with keys "cid" and "contribsys_id"
    """
    select_zephir = construct_select_zephir_cluster_by_contribsys_id(list_to_str(contribsys_id_list))
    if select_zephir:
        try:
            results = zephirDb.findall(text(select_zephir))
            return results
        except:
            return None
    return None

def find_zephir_clusters_by_cid_and_contribsys_ids(zephirDb, cid, contribsys_id_list):
    """
    Args:
        db_conn_str: database connection string
        cid: a CID
        contribsys_id_list: list of contribsys IDs in string
    Returns:
        list of dict with keys "cid" and "contribsys_id"
    """
    select_zephir = construct_select_zephir_cluster_by_cid_and_contribsys_id(cid, list_to_str(contribsys_id_list))
    if select_zephir:
        try:
            results = zephirDb.findall(text(select_zephir))
            return results
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

    db_conn_str = str(utils.db_connect_url(configs[env]))
    zephirDb = ZephirDatabase(db_conn_str)

    ocns_list = [6758168, 15437990, 5663662, 33393343, 28477569, 8727632]
    print("Inquiry OCNs: {}".format(ocns_list))
    results = zephir_clusters_lookup(zephirDb, ocns_list)
    print(results)

    sysid_list = ['pur63733', 'nrlf.b100608668']
    print("Inquiry sys IDs: {}".format(sysid_list))
    results = find_zephir_clusters_by_contribsys_ids(zephirDb, sysid_list)
    print(results)

    cid = "000000009"
    sysid_list = ['miu000000009', 'miu.000000009']
    print("Inquiry ids: CID: {}, sys IDs: {}".format(cid, sysid_list))
    results = find_zephir_clusters_by_cid_and_contribsys_ids(zephirDb, cid, sysid_list)
    print(results)

if __name__ == '__main__':
    main()
