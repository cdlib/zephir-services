import os
import sys

import environs
import re
import datetime

from sqlalchemy import create_engine
from sqlalchemy import text
from pandas import DataFrame

import lib.utils as utils
from config import get_configs_by_filename

""" performance note:
autoid between 1 and 100,000: 1.3 sec
autoid between 1 and 1,000,000: 36 sec
autoid between 1 and 10,000,000: killed
"""
SELECT_ZEPHIR_IDS = """select zr.autoid as zr_autoid, cid, identifier as oclc, contribsys_id, id as htid  
  from zephir_records zr
  inner join zephir_identifier_records zir on zir.record_autoid = zr.autoid
  inner join zephir_identifiers zi on zir.identifier_autoid = zi.autoid
  where zr.autoid between :start_autoid and :end_autoid
  and zi.type = 'oclc'
  group by cid, identifier, id
  order by cid, id, identifier
"""

SELECT_MAX_ZEPHIR_AUTOID = "select max(autoid) as max_autoid from zephir_records"

SELECT_MARCXML_BY_ID = "SELECT metadata FROM zephir_filedata"

def construct_select_marcxml_by_id(id):
    if id:
        return SELECT_MARCXML_BY_ID + " WHERE id = '" + id + "'"
    else:
        return None


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
    select_zephir = construct_select_marcxml_by_id(id)
    return find_zephir_records(db_connect_str, select_zephir)

def find_max_zephir_autoid(db_connect_str):
    return find_zephir_records(db_connect_str, SELECT_MAX_ZEPHIR_AUTOID)

def test_zephir_search(db_connect_str):

    id = "pur1.32754075735872"
    results = find_marcxml_records_by_id(db_connect_str, id)
    for result in results:
        print (result)


def main():
    if (len(sys.argv) > 1):
        env = sys.argv[1]
    else:
        env = "dev"

    configs= get_configs_by_filename('config', 'zephir_db')
    db_connect_str = str(utils.db_connect_url(configs[env]))

    #test_zephir_search(db_connect_str)
    #exit()

    if len(sys.argv) > 2:
        input_filename = sys.argv[2]
    else:
        input_filename = "./data/htids.txt"
    if len(sys.argv) > 3:
        output_filename = sys.argv[3]
    else:
        output_filename = "./output/marc_records.xml"

    results = find_max_zephir_autoid(db_connect_str)
    if (results):
        max_zephir_autoid = results[0]["max_autoid"]
        print("max zephir autoid: {}".format(max_zephir_autoid))

    #output_xmlrecords(input_filename, output_filename, db_connect_str)

    readSqlToDataframe(db_connect_str, max_zephir_autoid)


def output_xmlrecords(input_filename, output_filename, db_connect_str):
    outfile = open(output_filename, 'w')
    outfile.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
    outfile.write("<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");

    with open(input_filename) as infile:
        for line in infile:
            id = line.strip()
            records = find_marcxml_records_by_id(db_connect_str, id)
            for record in records:
                marcxml = re.sub("<\?xml version=\"1.0\" encoding=\"UTF-8\"\?>\n", "", record["metadata"])
                marcxml = re.sub(" xmlns=\"http://www.loc.gov/MARC21/slim\"", "", marcxml)
                outfile.write(marcxml)

    outfile.write("</collection>\n")
    outfile.close()

    print("marcxml records are save in file: {}".format(output_filename))

def readSqlToDataframe(db_connect_str, max_zephir_autoid=None):

    df = None 
    start_autoid = 0 
    end_autoid = 0 
    step = 100000
    while max_zephir_autoid and end_autoid <= max_zephir_autoid + step:
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
        
        if df is None:
            df = DataFrame(records)
        else:
            df_2 = None
            df_2 = DataFrame(records)
            df.append(df_2, ignore_index = True)

    print(df.head())
    print("rows in dataframe: {}".format(len(df)))



if __name__ == '__main__':
    main()
