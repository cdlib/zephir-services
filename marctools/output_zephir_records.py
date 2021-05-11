import os
import sys

import environs
import re
import datetime

from sqlalchemy import create_engine
from sqlalchemy import text

import numpy as np
import math
import locale

import pandas as pd
from pandas import DataFrame

import lib.utils as utils
from config import get_configs_by_filename

""" performance note:
autoid between 1 and 100,000: 1.3 sec
autoid between 1 and 1,000,000: 36 sec
autoid between 1 and 10,000,000: killed
"""
SELECT_ZEPHIR_IDS = """select CAST(cid as UNSIGNED) cid, identifier as oclc, zr.autoid as z_record_autoid 
  from zephir_records zr
  inner join zephir_identifier_records zir on zir.record_autoid = zr.autoid
  inner join zephir_identifiers zi on zir.identifier_autoid = zi.autoid
  where zr.autoid between :start_autoid and :end_autoid
  and zi.type = 'oclc'
  group by cid, identifier, id
  order by cid, id, identifier
"""

SELECT_MAX_ZEPHIR_AUTOID = "select max(autoid) as max_autoid from zephir_records"

SELECT_MARCXML_BY_AUTOID = "SELECT metadata FROM zephir_filedata WHERE autoid =:autoid"

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

    print("Get Zephir Item Details")
    #raw_zephir_item_detail = getZephirItemDetailsDataFrame(db_connect_str)
    raw_zephir_item_detail_path = "./output/zephir_items-stg.csv"
    raw_zephir_item_detail = pd.read_csv(raw_zephir_item_detail_path, names=["cid", "oclc", "z_record_autoid"], header=None, dtype={"cid":int, "oclc":object, "z_record_autoid":int}, error_bad_lines=False)
    raw_zephir_item_detail.info()
    raw_zephir_item_detail.head()

    print("Cleanup Data")
    raw_zephir_item_detail = cleanupData(raw_zephir_item_detail)
    raw_zephir_item_detail.info()
    raw_zephir_item_detail.head()

    print("Get Concordance")
    zephir_concordance_path = "data/zephir_concordance.csv"
    zephir_concordance_df = readCsvFileToDataFrame(zephir_concordance_path)

    print("Find htids for deduplicate clusters")
    htids_df = find_htids_for_deduplicate_clusters(zephir_concordance_df, raw_zephir_item_detail)

    print("Output Zephir records in XML")
    output_xmlrecords(htids_df, output_filename, db_connect_str)

def output_xmlrecords(htids_df, output_filename, db_connect_str):
    outfile = open(output_filename, 'w')
    outfile.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
    outfile.write("<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");

    for index in htids_df.index:
        autoid = htids_df['z_record_autoid'][index]
        records = find_marcxml_records_by_autoid(db_connect_str, autoid)
        for record in records:
            marcxml = re.sub("<\?xml version=\"1.0\" encoding=\"UTF-8\"\?>\n", "", record["metadata"])
            marcxml = re.sub(" xmlns=\"http://www.loc.gov/MARC21/slim\"", "", marcxml)
            outfile.write(marcxml)

    outfile.write("</collection>\n")
    outfile.close()

    print("marcxml records are save in file: {}".format(output_filename))

def output_xmlrecords_file_version(input_filename, output_filename, db_connect_str):
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

def getZephirItemDetailsDataFrame(db_connect_str):

    max_zephir_autoid = find_max_zephir_autoid(db_connect_str)
    #max_zephir_autoid = 10000

    zephir_items_file = "output/zephir_items.csv"
    # create an empty file
    open(zephir_items_file, 'w').close()

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
        #df = None

    df= pd.read_csv(zephir_items_file, names=["cid", "oclc", "z_record_autoid"], header=None, dtype={"cid":int, "oclc":object, "z_record_autoid":int}, error_bad_lines=False)
    print(df.info())
    print(df.head())
    return df

def readCsvFileToDataFrame(file_path):
    zephir_concordance_df = pd.read_csv(file_path, names=["primary","oclc"], header=0)
    print(zephir_concordance_df.info())
    print(zephir_concordance_df.head())
    return zephir_concordance_df

def cleanupData(zephir_item_detail):
    print("CLEANUP DATA")
    # Step 5 - CLEANUP DATA
    # coerce identifier data from objects, to numeric
    zephir_item_detail["oclc"] = zephir_item_detail["oclc"].apply(lambda x: int(x) if str(x).isdigit() else None)
    zephir_item_detail["oclc"] = zephir_item_detail["oclc"].apply(pd.to_numeric, errors='coerce')

    # drop null rows
    zephir_item_detail = zephir_item_detail.dropna()

    # cast data as integers (the "to_numberic" causes change in type) - drops leading zeros
    zephir_item_detail["oclc"] = zephir_item_detail["oclc"].astype(int)

    zephir_item_detail.info()
    zephir_item_detail.head()

    return zephir_item_detail

def find_htids_for_deduplicate_clusters(zephir_concordance_df, zephir_item_detail):
    """Returns a dataframe with Zephir_Records AUTOIDs.

    Keyword arguments:
    zephir_concordance_df - Dataframe containing oclc number and its resolved primary oclc number
    zephir_item_detail - Dataframe containing selected fields from the zephir_records table (cid, oclc, autoid)
    Note:
      The initial plan was to return HTIDs. We switched to use AUTOID to reduce the size of dataset.
      This won't affect the final results as AUTOID and HTIDs are in one to one relationship.
    Step:
        1. Join the two data frames on the oclc number field
        2. Find the primary oclc numbers with multiple CIDs
        3. Find the higher CIDs for the same primary oclc number
        4. Find the AUTOIDs associated to the selected CIDs
        5. Return the AUTOIDs 
    """
    """moved out and run as prep-step
    print("Step 5 - CLEANUP DATA")
    # Step 5 - CLEANUP DATA
    # coerce identifier data from objects, to numeric
    zephir_item_detail["oclc"] = zephir_item_detail["oclc"].apply(lambda x: int(x) if str(x).isdigit() else None)
    zephir_item_detail["oclc"] = zephir_item_detail["oclc"].apply(pd.to_numeric, errors='coerce')

    # drop null rows
    zephir_item_detail = zephir_item_detail.dropna()

    # cast data as integers (the "to_numberic" causes change in type) - drops leading zeros
    zephir_item_detail["oclc"] = zephir_item_detail["oclc"].astype(int)

    zephir_item_detail.info()
    zephir_item_detail.head()
    """

    print("Step 6 - Analysis - join DFs by oclc")
    # Step 6 - Analysis
    # Create analysis table by joining with zephir concordance
    # this results in a zephir table with both the original oclc and the primary oclc
    analysis_df = zephir_item_detail.merge(zephir_concordance_df, on='oclc', how='left')
    zephir_concordance_df = None
    analysis_df["oclc"] = analysis_df["oclc"].astype('Int64')
    analysis_df["primary"] = analysis_df["primary"].astype('Int64')
    analysis_df.info()
    analysis_df.head()

    print("Step 7 - Find primary numbers with a CID count> 1")
    # Step 7 - Find primary numbers with a CID count> 1
    df = analysis_df.copy()
    df = df[['cid','primary']]
    df = df.dropna()
    df = df[~df.duplicated(subset=['cid','primary'],keep='first')]
    # create a column with the count of cids per primary oclc row
    df = df.groupby(["primary"]).size().reset_index().rename(columns={0:'cid_count'})
    # create a subset of primary numbers where cid count is greater than 1
    df = df[df['cid_count'] > 1]
    df.info()
    print(df.head())
    # save for later in specific dataframe
    df_primary_with_duplicates = df
    df = None # clear out for next analysis

    print("Step 8 - create a subset of analysis data with only primary numbers that have >1 CID assoicated using a join")
    # Step 8 - create a subset of analysis data with only primary numbers that have >1 CID assoicated using a join
    df = analysis_df.dropna().merge(df_primary_with_duplicates, on='primary', how='right')
    df.sort_values(by=['primary', 'cid'])
    df.info()
    df.head(30)

    print("Step 10 - create lookup table for the lowest CID per primary number")
    # Step 10 - create lookup table for the lowest CID per primary number 
    lowest_cid_df = df[~df.duplicated(subset=['primary'],keep='first')][["primary","cid"]]
    lowest_cid_df = dict(zip(lowest_cid_df.primary, lowest_cid_df.cid))
    # preserve rows where the cid is higher than the first cid matching the OCLC
    dups = []
    for i, row in df.iterrows():
        dups.append(row['cid'] > lowest_cid_df[row["primary"]])

    print("Step 11 - create a dataframe subset of all the duplicate CID-HTIDs")
    # Step 11 - create a dataframe subset of all the duplicate CID-HTIDs
    # Note: Some duplicate CIDs may have additional records with a different OCLC
    higher_cid_duplicates_df = df[dups]
    higher_cid_duplicates_df.info()
    higher_cid_duplicates_df.head(30)

    print("Step 12 - select a dataframe with only htids from cids with higher cid values")
    # Step 12 - select a dataframe with only htids from cids with higher cid values
    htid_duplicates_df = higher_cid_duplicates_df[["z_record_auotid"]]
    htid_duplicates_df.head(30)
    # save dataset to csv
    htid_duplicates_df.to_csv("output/z_record_autoids.csv", index=False)
    return htid_duplicates_df


if __name__ == '__main__':
    main()
