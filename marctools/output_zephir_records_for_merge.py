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

import click

import lib.utils as utils
from config import get_configs_by_filename

from zephir_db_utils import createZephirItemDetailsFileFromDB
from zephir_db_utils import find_marcxml_records_by_autoid 

def test_zephir_search(db_connect_str):

    id = "pur1.32754075735872"
    results = find_marcxml_records_by_id(db_connect_str, id)
    for result in results:
        print (result)


@click.command()
@click.option('-e', '--env', default="dev")
@click.option('-H', '--input-htid-file')
@click.option('-S', '--search-zephir-database', is_flag=True, help="Get Zephir items data from database.")
@click.option('-Z', '--zephir-items-file', default="./data/zephir_items.csv")
@click.option('-C', '--oclc-concordance-file', default="./data/zephir_concordance.csv")
@click.option('-o', '--output-marc-file', default="./output/marc_records_for_reload.xml")
def main(env, input_htid_file, search_zephir_database,
        zephir_items_file, oclc_concordance_file, output_marc_file):
    print(env)
    print(input_htid_file)
    print(search_zephir_database)
    print(zephir_items_file)
    print(oclc_concordance_file)
    print(output_marc_file)

    configs= get_configs_by_filename('config', 'zephir_db')
    db_connect_str = str(utils.db_connect_url(configs[env]))

    #test_zephir_search(db_connect_str)
    #exit()

    if input_htid_file:
        print("Output marc records from HTIDs defined in: {}".format(input_htid_file))
        output_xmlrecords_by_htid(input_htid_file, output_marc_file, db_connect_str)
        print("The marcxml records are save in file: {}".format(output_marc_file))
        print("Finished processing.")
        exit()

    if search_zephir_database:
        print("Get Zephir item details from the database")
        print("Data will be saved in file {}".format(zephir_items_file))
        createZephirItemDetailsFileFromDB(db_connect_str, zephir_items_file)
    else:
        print("Get Zephir item details from prepared file: {}".format(zephir_items_file))

    print("Zephir item data contains fields: cid, oclc, contribsys_id, htid, z_record_autoid")
    print("The data file does not contain a header line.")

    f_output_marc_file(zephir_items_file, oclc_concordance_file, output_marc_file)
    print("Records for merge are saved in file: {}".format(output_marc_file))

def f_output_marc_file(zephir_items_file, oclc_concordance_file, output_marc_file):

    # 543 MB
    print("Get Zephir Item Details: cid, oclc, contribsys_id, htid, z_record_autoid")
    print("Read in data to DF: cid, oclc, z_record_autoid")

    raw_zephir_item_detail = pd.read_csv(zephir_items_file, header=0, usecols=[0, 1, 4], names=["cid", "oclc", "z_record_autoid"], dtype={"cid":int, "oclc":object, "z_record_autoid":int}, error_bad_lines=False)

    # 724 MB
    print("Cleanup Data")
    raw_zephir_item_detail = cleanupData(raw_zephir_item_detail)

    # 150 MB
    print("Get Concordance")
    zephir_concordance_df = readCsvFileToDataFrame(oclc_concordance_file)

    # 980 MB
    print("Join data frames")
    analysis_df = createAnalysisDataframe(zephir_concordance_df, raw_zephir_item_detail)
    del raw_zephir_item_detail
    del zephir_concordance_df

    print("Find primary numbers with a CID count> 1") 
    df_primary_with_duplicates = findOCNsWithMultipleCIDs(analysis_df)

    df = subsetOCNWithMultipleCIDs(analysis_df, df_primary_with_duplicates)
    del analysis_df
    del df_primary_with_duplicates

    print("Find htids for deduplicate clusters")
    autoid_file = "output/z_record_autoids.csv"
    find_htids_for_deduplicate_clusters(df, autoid_file)

    print("Output Zephir records in XML")
    output_xmlrecords(autoid_file, output_marc_file, db_connect_str)

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

    print("Step 12 - select a dataframe with only htids(autoid) from cids with higher cid values")
    # Step 12 - select a dataframe with only htids from cids with higher cid values
    htid_duplicates_df = higher_cid_duplicates_df[["z_record_autoid"]]
    print(htid_duplicates_df.info())
    print(htid_duplicates_df.head(30))
    # save dataset to csv
    htid_duplicates_df.to_csv(output_file, index=False, header=False)


if __name__ == '__main__':
    main()
    #temp_run_output_xml_only()
