import os
import sys
import re

from sqlalchemy import create_engine
from sqlalchemy import text

from lib.utils import db_connect_url
from lib.utils import get_configs_by_filename

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
    if invalid_sql_in_clause_str(contribsys_ids):
        return None

    return "SELECT distinct cid, contribsys_id FROM zephir_records WHERE contribsys_id in (" + contribsys_ids + ") order by cid"

def construct_select_contribsys_id_by_cid(cids):
    if invalid_sql_in_clause_str(cids):
        return None
    
    return "SELECT distinct cid, contribsys_id FROM zephir_records WHERE cid in (" + cids + ") order by cid"

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
        zephirDb: ZephirDatabase class
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
    cid_ocn_clusters = formatting_cid_id_clusters(cid_ocn_list, "ocn")

    zephir_cluster = {
        "inquiry_ocns_zephir": ocns_list,
        "cid_ocn_list": cid_ocn_list,
        "cid_ocn_clusters": cid_ocn_clusters,
        "num_of_matched_zephir_clusters": len(cid_ocn_clusters),
        "min_cid": min([cid_ocn.get("cid") for cid_ocn in cid_ocn_list])
    }
    return zephir_cluster

def zephir_clusters_lookup_by_sysids(zephirDb, sysids_list):
    """
    Finds Zephir clusters by sysids and returns clusters' info including cluster IDs, number of clusters and all SysIds in each cluster. 
    Args:
        zephirDb: ZephirDatabase class
        sysids_list: list of sysids in string
    Return: A dict with:
        "inquiry_sysids": input sysids list,
        "cid_sysid_list": list of dict with keys of "cid" and "sysid",
        "cid_sysid_clusters": dict with key="cid", value=list of sysids in the cid cluster,
        "num_of_matched_zephir_clusters": number of matched clusters
        "min_cid": lowest CID 
    """
    zephir_cluster = {
        "inquiry_sysids": sysids_list,
        "cid_sysid_list": [],
        "cid_sysid_clusters": {},
        "num_of_matched_zephir_clusters": 0,
        "min_cid": None,
    }

    cid_sysid_list = find_zephir_clusters_by_contribsys_ids(zephirDb, sysids_list)
    if not cid_sysid_list:
        return zephir_cluster

    # find all sysids in each cluster
    cids_list = [cid_sysid.get("cid") for cid_sysid in cid_sysid_list]
    unique_cids_list = list(set(cids_list))
    cid_sysid_list_2 = find_zephir_clusters_and_contribsys_ids_by_cid(zephirDb, unique_cids_list)
    if not cid_sysid_list_2:
        return zephir_cluster

    # convert to a dict with key=cid, value=list of sysids
    cid_sysid_clusters = formatting_cid_id_clusters(cid_sysid_list_2, "contribsys_id")

    zephir_cluster = {
        "inquiry_sysids": sysids_list,
        "cid_sysid_list": cid_sysid_list,
        "cid_sysid_clusters": cid_sysid_clusters,
        "num_of_matched_zephir_clusters": len(cid_sysid_clusters),
        "min_cid": min([cid_sysid.get("cid") for cid_sysid in cid_sysid_list])
    }
    return zephir_cluster

def find_zephir_clusters_by_ocns(zephirDb, ocns_list):
    """
    Args:
        zephirDb: ZephirDatabase class
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
        zephirDb: ZephirDatabase class
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
        zephirDb: ZephirDatabase class 
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

def find_zephir_clusters_and_contribsys_ids_by_cid(zephirDb, cid_list):
    """
    Args:
        zephirDb: ZephirDatabase class
        cid: a CID
        contribsys_id_list: list of contribsys IDs in string
    Returns:
        list of dict with keys "cid" and "contribsys_id"
    """
    select_zephir = construct_select_contribsys_id_by_cid(list_to_str(cid_list))
    if select_zephir:
        try:
            results = zephirDb.findall(text(select_zephir))
            return results
        except:
            return None
    return None


def list_to_str(a_list):
    """Convert list items to single quoted and comma separated string for MySQL IN Clause use.
       Replace single quotes in the original list item to two single quotes so it can be matched by MySQL.
    """
    new_str = ""
    for item in a_list:
        item = str(item).replace("\'", "\'\'")
        if new_str:
            new_str += ", '" + item + "'"
        else:
            new_str = "'" + item + "'"
    return new_str

def formatting_cid_id_clusters(cid_id_list, other_id):
    """Put cid and id pairs into clusters by unique cids. 
    Args:
        cid_ocn_list: list of dict with keys of "cid" and another ID such as "ocn" or "sysid".
        [{"cid": cid1, "ocn": ocn1}, {"cid": cid1, "ocn": ocn2}, {"cid": cid3, "ocn": ocn3}]
        other_id: other ID such as "ocn", "contribsys_id"
    Returns:
        A dict with key=unique cid, value=list of other id with the same cid.
        {"cid1": [ocn1, ocn2],
         "cid3", [ocn3]}
    """
    # key: cid, value: list of ocns [ocn1, ocn2]
    cid_ids_dict = {}

    if cid_id_list:
        for cid_id in cid_id_list:
            cid = cid_id.get("cid")
            id = cid_id.get(other_id)
            if cid in cid_ids_dict:
                cid_ids_dict[cid].append(id)
            else:
                cid_ids_dict[cid] = [id]

    return cid_ids_dict

def valid_sql_in_clause_str(input_str):
    """Validates if input is comma separated, single quoted strings.

    Returns:
        True: valid
        False: Invalid

        For example:
        True: 
            "'1'"
            "'1', '2345'"
            "'a'"
            "'abc', 'xyz'"
        False: 
            "1, 2345"
            ""
            " "
    """

    if not input_str:
        return False

    if re.search(r"^(\s)*'(.+)'(\s)*((\s)*(,)(\s)*('(.+)'))*$", input_str):
        return True
    
    return False

def invalid_sql_in_clause_str(input_str):
    return not valid_sql_in_clause_str(input_str)

def main():
    if (len(sys.argv) > 1):
        env = sys.argv[1]
    else:
        env = "test"

    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, 'config')

    configs= get_configs_by_filename(CONFIG_PATH, 'zephir_db')
    print(configs)

    db_conn_str = str(db_connect_url(configs[env]))
    zephirDb = ZephirDatabase(db_conn_str)

    ocns_list = [6758168, 15437990, 5663662, 33393343, 28477569, 8727632]
    print("Inquiry OCNs: {}".format(ocns_list))
    results = zephir_clusters_lookup(zephirDb, ocns_list)
    print(results)

    sysid_list = ['pur63733', 'nrlf.b100608668']
    results = zephir_clusters_lookup_by_sysids(zephirDb, sysid_list)
    print(results)

if __name__ == '__main__':
    main()
