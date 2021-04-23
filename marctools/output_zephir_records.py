import os
import sys
import environs
import re

from sqlalchemy import create_engine
from sqlalchemy import text
from pandas import DataFrame

import lib.utils as utils
from config import get_configs_by_filename

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

def find_zephir_records(db_conn_str, select_query):
    """
    Args:
        db_conn_str: database connection string
        sql_select_query: SQL select query
    Returns:
        list of dict with selected field names as keys
    """
    if select_query:
        try:
            zephir = ZephirDatabase(db_conn_str)
            results = zephir.findall(text(select_query))
            zephir.close()
            return results
        except:
            return None
    return None

def find_marcxml_records_by_id(db_conn_str, id):
    """
    Args:
        db_conn_str: database connection string
        id: htid in string
    Returns:
        list of dict with marcxml 
    """
    select_zephir = construct_select_marcxml_by_id(id)
    return find_zephir_records(db_conn_str, select_zephir)

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

    if len(sys.argv) > 2:
        input_filename = sys.argv[2]
    else:
        input_filename = "./data/htids.txt"
    if len(sys.argv) > 3:
        output_filename = sys.argv[3]
    else:
        output_filename = "./output/marc_records.xml"

    output_xmlrecords(input_filename, output_filename, db_connect_str)

    readSqlToDataframe(db_connect_str)


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

def readSqlToDataframe(db_connect_str):

    select_query = "select cid, id, contribsys_id from zephir_records limit 5 "
    records = find_zephir_records(db_connect_str, select_query)
    print(records)

    df = DataFrame(records)
    print(type(df))
    print(df)



if __name__ == '__main__':
    main()
