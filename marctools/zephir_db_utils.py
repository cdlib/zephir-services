import os
import sys

import datetime

from sqlalchemy import create_engine
from sqlalchemy import text

import pandas as pd
from pandas import DataFrame

SELECT_ZEPHIR_IDS = """select CAST(cid as UNSIGNED) cid, identifier as oclc,
  zr.contribsys_id as contribsys_id, zr.id as htid, zr.autoid as z_record_autoid
  from zephir_records zr
  inner join zephir_identifier_records zir on zir.record_autoid = zr.autoid
  inner join zephir_identifiers zi on zir.identifier_autoid = zi.autoid
  where zr.autoid between :start_autoid and :end_autoid
  and zi.type = 'oclc'
  order by cid, id, identifier
"""

SELECT_MAX_ZEPHIR_AUTOID = "select max(autoid) as max_autoid from zephir_records"

SELECT_MARCXML_BY_AUTOID = """SELECT metadata, zephir_records.id FROM zephir_filedata
  join zephir_records on zephir_records.id = zephir_filedata.id
  WHERE zephir_records.autoid =:autoid
"""

SELECT_MARCXML_BY_ID = "SELECT metadata FROM zephir_filedata WHERE id=:id"

SELECT_HTID_BY_AUTOID = "SELECT autoid, id FROM zephir_records WHERE autoid =:autoid"

class ZephirDatabase:
    def __init__(self, db_connect_str):
        self.engine = create_engine(db_connect_str)

    def findall(self, sql, params=None):
        with self.engine.connect() as connection:
            results = connection.execute(sql, params or ())
            results_dict = [dict(row) for row in results.fetchall()]
            return results_dict

    def close(self):
        self.engine.dispose()

def find_zephir_records(db_connect_str, select_query, params=None):
    """
    Args:
        db_connect_str: database connection string
        sql_select_query: SQL select query
    Returns:
        list of dict with selected field names as keys
    """
    if select_query:
        try:
            zephir = ZephirDatabase(db_connect_str)
            results = zephir.findall(text(select_query), params)
            zephir.close()
            return results
        except:
            return None
    return None


def find_marcxml_records_by_id(db_connect_str, id):
    """
    Args:
        db_connect_str: database connection string
        id: htid in string
    Returns:
        list of dict with marcxml
    """
    params = {"id": id}
    return find_zephir_records(db_connect_str, SELECT_MARCXML_BY_ID, params)

def find_marcxml_records_by_autoid(db_connect_str, autoid):
    """
    Args:
        db_connect_str: database connection string
        autoid: integer
    Returns:
        list of dict with marcxml
    """
    params = {"autoid": autoid}
    return find_zephir_records(db_connect_str, SELECT_MARCXML_BY_AUTOID, params)

def find_max_zephir_autoid(db_connect_str):
    max_zephir_autoid = None
    results = find_zephir_records(db_connect_str, SELECT_MAX_ZEPHIR_AUTOID)
    if (results):
        max_zephir_autoid = results[0]["max_autoid"]
        print("max zephir autoid: {}".format(max_zephir_autoid))
    return max_zephir_autoid

def find_htid_by_autoid(db_connect_str, autoid):
    params = {"autoid": autoid}
    return find_zephir_records(db_connect_str, SELECT_HTID_BY_AUTOID, params)


def createZephirItemDetailsFileFromDB(db_connect_str, zephir_items_file):

    # create an empty file
    open(zephir_items_file, 'w').close()

    max_zephir_autoid = find_max_zephir_autoid(db_connect_str)
    #max_zephir_autoid = 10000

    start_autoid = 0
    end_autoid = 0
    step = 100000
    #step = 1000
    while max_zephir_autoid and end_autoid < max_zephir_autoid:
        start_autoid = end_autoid + 1
        end_autoid = start_autoid + step -1
        print("start: {}".format(start_autoid))
        print("end: {}".format(end_autoid))

        current_time = datetime.datetime.now()
        print(current_time)
        params = {"start_autoid": start_autoid, "end_autoid": end_autoid}
        records = find_zephir_records(db_connect_str, SELECT_ZEPHIR_IDS, params)

        current_time = datetime.datetime.now()
        print(current_time)

        df = DataFrame(records)
        df.to_csv(zephir_items_file, mode='a', header=False, index=False)
