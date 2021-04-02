import os
import sys
import environs
import re

import csv
from sqlalchemy import create_engine
from sqlalchemy import text

import lib.utils as utils
from config import get_configs_by_filename
from compare_records import FuzzyRatios

SELECT_TITLES = """SELECT DISTINCT 
    cid, contribsys_id, CONCAT_WS(", ", title, creator, publisher) as title_key,
    substr(json_unquote(metadata_json->'$.fields[5]."008"'), 36, 3) as lang
    from zephir_records z
    join zephir_filedata f on z.id = f.id
"""

def construct_select_zephir_titles_by_cid(cid):
    if cid:
        return SELECT_TITLES + " WHERE cid = '" + cid + "'"
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

def find_zephir_titles_by_cid(db_conn_str, cid):
    """
    Args:
        db_conn_str: database connection string
        cid_list: list of CIDs in string
    Returns:
        list of dict with keys "cid" and "ocn"
    """
    select_zephir = construct_select_zephir_titles_by_cid(cid)
    return find_zephir_records(db_conn_str, select_zephir) 

def test_zephir_search(db_connect_str):
    cid = "000000001"
    results = find_zephir_titles_by_cid(db_connect_str, cid)
    for result in results:
        print (result)

def test_match():
    str_a = "a"
    str_b= "A "
    compare = FuzzyRatios(str_a, str_b)
    print (str_a)
    print (str_b)
    print ("fuzzy_ratio {}".format(compare.fuzzy_ratio))
    print ("fuzzy_partial_ratio {}".format(compare.fuzzy_partial_ratio))
    print ("fuzzy_token_sort_ratio {}".format(compare.fuzzy_token_sort_ratio))
    print ("fuzzy_token_set_ratio {}".format(compare.fuzzy_token_set_ratio))

def main():
    if (len(sys.argv) > 1):
        env = sys.argv[1]
    else:
        env = "dev"

    configs= get_configs_by_filename('config', 'zephir_db')
    db_connect_str = str(utils.db_connect_url(configs[env]))

    #test_zephir_search(db_connect_str)
    #test_match

    if len(sys.argv) > 2:
        input_file = sys.argv[2]
    else:
        input_file = "./data/cids_with_multi_primary_ocns.csv"
    if len(sys.argv) > 3:
        output_file = sys.argv[3]
    else:
        output_file = "./output/cids_with_multi_primary_ocns_similarity_scores.csv"

    csv_columns = ["cid", "contribsys_id", "flag", "title_key", "lang", "similarity_ratio", "partial_ratio", "token_sort", "token_set"]

    count = 0
    with open(input_file) as infile, open(output_file, 'w') as outfile:
        reader = csv.reader(infile)
        next(reader, None)  # skip the headers
        writer = csv.DictWriter(outfile, fieldnames=csv_columns)
        writer.writeheader()

        for fields in reader:
            count += 1
            if len(fields) > 0:
                # left padding 0s to CID
                cid = ("000000000" + fields[0])[-9:]
                #print (cid)
                results = find_zephir_titles_by_cid(db_connect_str, cid)
                first_item = True
                for result in results:
                    if first_item:
                        title_key = result["title_key"]
                        first_item = False
                        result_base = {
                            "cid": result["cid"],
                            "contribsys_id": result["contribsys_id"],
                            "flag": "B",
                            "title_key" : result["title_key"],
                            "lang" : result["lang"].decode() if result["lang"] else "",
                        }
                    else:
                        ratios = FuzzyRatios(title_key, result["title_key"])
                        #print (ratios.fuzzy_ratio)
                        result_pair = {
                            "cid": result["cid"],
                            "contribsys_id": result["contribsys_id"],
                            "flag": "",
                            "title_key" : result["title_key"],
                            "lang" : result["lang"].decode() if result["lang"] else "",
                            "similarity_ratio": ratios.fuzzy_ratio,
                            "partial_ratio": ratios.fuzzy_partial_ratio,
                            "token_sort": ratios.fuzzy_token_sort_ratio,
                            "token_set": ratios.fuzzy_token_set_ratio,
                        }
                        if (ratios.fuzzy_ratio < 100):
                            writer.writerow(result_base)
                            writer.writerow(result_pair)


if __name__ == '__main__':
    main()
