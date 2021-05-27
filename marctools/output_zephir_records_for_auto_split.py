import os
import sys

import environs
import re
import datetime

import numpy as np
import math
import locale

import pandas as pd
from pandas import DataFrame

import lib.utils as utils
from config import get_configs_by_filename

from zephir_db_utils import find_zephir_records

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

SELECT_ZEPHIR_IDS_2 = """select CAST(cid as UNSIGNED) cid, identifier as oclc, 
  zr.contribsys_id as contribsys_id, zr.autoid as z_record_autoid 
  from zephir_records zr
  inner join zephir_identifier_records zir on zir.record_autoid = zr.autoid
  inner join zephir_identifiers zi on zir.identifier_autoid = zi.autoid
  where zr.autoid between :start_autoid and :end_autoid
  and zi.type = 'oclc'
  group by cid, identifier, id
  order by cid, id, identifier

SELECT_MAX_ZEPHIR_AUTOID = "select max(autoid) as max_autoid from zephir_records"

SELECT_MARCXML_BY_AUTOID = """SELECT metadata FROM zephir_filedata
  join zephir_records on zephir_records.id = zephir_filedata.id 
  WHERE zephir_records.autoid =:autoid
"""

SELECT_MARCXML_BY_ID = "SELECT metadata FROM zephir_filedata"

def construct_select_marcxml_by_id(id):
    if id:
        return SELECT_MARCXML_BY_ID + " WHERE id = '" + id + "'"
    else:
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

    # 543 MB
    print("Get Zephir Item Details")
    #raw_zephir_item_detail = getZephirItemDetailsDataFrame(db_connect_str)
    raw_zephir_item_detail_path = "./output/zephir_items-stg.csv"
    raw_zephir_item_detail = pd.read_csv(raw_zephir_item_detail_path, names=["cid", "oclc", "z_record_autoid"], header=None, dtype={"cid":int, "oclc":object, "z_record_autoid":int}, error_bad_lines=False)

    # 724 MB
    print("Cleanup Data")
    raw_zephir_item_detail = cleanupData(raw_zephir_item_detail)

    # 150 MB
    print("Get Concordance")
    zephir_concordance_path = "data/zephir_concordance.csv"
    zephir_concordance_df = readCsvFileToDataFrame(zephir_concordance_path)

    # 980 MB
    print("Join data frames")
    analysis_df = createAnalysisDataframe(zephir_concordance_df, raw_zephir_item_detail)
    del raw_zephir_item_detail
    del zephir_concordance_df

    print("Find CIDs with multiple OCLC primary numbers") 
    cids_with_multi_primary_fc_df = findCIDsWithMultipleOCNs(analysis_df)
    cids_with_multi_primary_fc_count = len(cids_with_multi_primary_fc_df)

    print("Find Contributor system IDs with multiple OCLC primary numbers")
    contribsys_ids_with_multi_primary_fc_df = findContribIDsWithWithMultipleOCNs(analysis_df)

    del analysis_df

    print("CIDs with multiple OCLCs, but no overlapping contribsys records")
    auto_splitable_cids = subsetCIDsWithMultipleOCNs(cids_with_multi_primary_fc_df, contribsys_ids_with_multi_primary_fc_df)

    del cids_with_multi_primary_fc_df
    del contribsys_ids_with_multi_primary_fc_df

    autoid_file = "output/z_record_autoids.csv"
    # save dataset to csv
    auto_splitable_cids.to_csv(autoid_file, index=False, header=False)

    print("Output Zephir records in XML")
    output_xmlrecords(autoid_file, output_filename, db_connect_str)

def temp_run_output_xml_only():
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
        input_filename = "/data/htids.txt"
    if len(sys.argv) > 3:
        output_filename = sys.argv[3]
    else:
        output_filename = "output/marc_records.xml"

    file_path = "output/z_record_autoids.csv"
    output_xmlrecords(file_path, output_filename, db_connect_str)


def output_xmlrecords_df_version(htids_df, output_filename, db_connect_str):
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

def output_xmlrecords(input_filename, output_filename, db_connect_str):
    outfile = open(output_filename, 'w')
    outfile.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
    outfile.write("<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");

    with open(input_filename) as infile:
        for line in infile:
            autoid = line.strip()
            records = find_marcxml_records_by_autoid(db_connect_str, autoid)
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
    # Step 5 - CLEANUP DATA
    # coerce identifier data from objects, to numeric
    zephir_item_detail["oclc"] = zephir_item_detail["oclc"].apply(lambda x: int(x) if str(x).isdigit() else None)
    zephir_item_detail["oclc"] = zephir_item_detail["oclc"].apply(pd.to_numeric, errors='coerce')

    # drop null rows
    zephir_item_detail = zephir_item_detail.dropna()

    # cast data as integers (the "to_numberic" causes change in type) - drops leading zeros
    zephir_item_detail["oclc"] = zephir_item_detail["oclc"].astype(int)

    print(zephir_item_detail.info())
    print(zephir_item_detail.head())

    return zephir_item_detail

def createAnalysisDataframe(zephir_concordance_df, zephir_item_detail):
    print("Step 6 - Analysis - join DFs by oclc")
    # Step 6 - Analysis
    # Create analysis table by joining with zephir concordance
    # this results in a zephir table with both the original oclc and the primary oclc
    analysis_df = zephir_item_detail.merge(zephir_concordance_df, on='oclc', how='left')
    analysis_df["oclc"] = analysis_df["oclc"].astype('Int64')
    analysis_df["primary"] = analysis_df["primary"].astype('Int64')
    
    print(analysis_df.info())
    print(analysis_df.head())

    return analysis_df 

def findOCNsWithMultipleCIDs(analysis_df):
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

    print(df.info())
    print(df.head())
    return df

def subsetOCNWithMultipleCIDs(analysis_df, df_primary_with_duplicates):
    print("Step 8 - create a subset of analysis data with only primary numbers that have >1 CID assoicated using a join")
    # Step 8 - create a subset of analysis data with only primary numbers that have >1 CID assoicated using a join
    df = analysis_df.dropna().merge(df_primary_with_duplicates, on='primary', how='right')
    df.sort_values(by=['primary', 'cid'])
    
    print(df.info())
    print(df.head(30))
    return df

def find_htids_for_deduplicate_clusters(df, output_file):
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
    print(higher_cid_duplicates_df.info())
    print(higher_cid_duplicates_df.head(30))

    print("Step 12 - select a dataframe with only htids from cids with higher cid values")
    # Step 12 - select a dataframe with only htids from cids with higher cid values
    htid_duplicates_df = higher_cid_duplicates_df[["z_record_autoid"]]
    print(htid_duplicates_df.info())
    print(htid_duplicates_df.head(30))
    # save dataset to csv
    htid_duplicates_df.to_csv(output_file, index=False, header=False)


def findCIDsWithMultipleOCNs(analysis_df):
    print("Step 7 - find CIDs with multiple OCNs")
    # Step 7 - find CIDs with multiple OCNs
    df = analysis_df.copy()
    df = df[['cid','primary']]
    df = df.dropna()
    df = df[~df.duplicated(subset=['cid','primary'],keep='first')]
    fc_cids = len(df["cid"].unique())
    df = df.groupby(["cid"]).size().reset_index().rename(columns={0:'primary_count'})
    df = df[df['primary_count'] > 1]

    print(df.info())
    print(df.head())
    return df

def findContribIDsWithWithMultipleOCNs(analysis_df):
    print("Step 8 - Contributor system IDs with multiple OCLC primary numbers")
    # Step 8 - find Contributor system IDs with multiple OCLC primary numbers
    df = analysis_df.copy()
    df = df[['cid','primary','contribsys_id']]
    df = df.dropna()
    df = df[~df.duplicated(subset=['cid','primary','contribsys_id'],keep='first')]
    df = df.groupby(["contribsys_id","cid"]).size().reset_index().rename(columns={0:'primary_count'})
    df = df[df['primary_count'] > 1]

    print(df.info())
    print(df.head())
    return df

def subsetCIDsWithMultipleOCNs(cid_df, contrib_df):
    df = cid_df[~cid_df["cid"].isin(contrib_df["cid"].unique())]
    print(df.info())
    print(df.head())
    return df


if __name__ == '__main__':
    main()
    #temp_run_output_xml_only()
