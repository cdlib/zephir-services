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
from zephir_db_utils import find_htid_by_autoid
from batch_output_zephir_records import output_xmlrecords_in_batch

debug_mode = False

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
@click.option('-d', '--debug', is_flag=True, help="Output debug information")
def main(env, input_htid_file, search_zephir_database,
        zephir_items_file, oclc_concordance_file, output_marc_file, debug):

    debug_mode = False
    if debug:
        debug_mode=True
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

    # 543 MB
    print("Read in data to DF: cid, oclc, z_record_autoid")
    raw_zephir_item_detail = pd.read_csv(zephir_items_file, header=0, usecols=[0, 1, 4], names=["cid", "oclc", "z_record_autoid"], dtype={"cid":int, "oclc":object, "z_record_autoid":int}, error_bad_lines=False)

    # 724 MB
    print("Cleanup Data")
    raw_zephir_item_detail = cleanupData(raw_zephir_item_detail)
    if debug_mode:
        print_out_df_info_and_head(raw_zephir_item_detail)

    # 150 MB
    print("Get Concordance")
    zephir_concordance_df = pd.read_csv(oclc_concordance_file, names=["oclc", "primary"], header=0)
    if debug_mode:
        print_out_df_info_and_head(zephir_concordance_df)
        print_out_df_element(zephir_concordance_df, 'primary', 569, "ocn-primary=569")
        print_out_df_element(zephir_concordance_df, 'oclc', 569, "ocn=569")
        print_out_df_element(zephir_concordance_df, 'primary', 51451923, "ocn-primary=51451923")
        print_out_df_element(zephir_concordance_df, 'oclc', 51451923, "oclc=51451923")
        print_out_df_element(zephir_concordance_df, 'oclc', 1335344, "oclc=1335344")

    # 980 MB
    print("Join data frames")
    analysis_df = createAnalysisDataframe(zephir_concordance_df, raw_zephir_item_detail)
    del raw_zephir_item_detail
    del zephir_concordance_df

    if debug_mode:
        print_out_df_info_and_head(analysis_df, 30)
        print_out_df_element(analysis_df, 'primary', 569, "ocn-primary=569 after JOIN")
        print_out_df_element(analysis_df, 'oclc', 569, "ocn=569")
        print_out_df_element(analysis_df, 'primary', 51451923, "ocn-primary=51451923 after JOIN")
        print_out_df_element(analysis_df, 'oclc', 51451923, "oclc=51451923")
        print_out_df_element(analysis_df, 'oclc', 1335344, "oclc=1335344")

    print("Find primary numbers with a CID count> 1") 
    df_primary_with_duplicates = findOCNsWithMultipleCIDs(analysis_df)

    if debug_mode:
        print_out_df_info_and_head(df_primary_with_duplicates)
        print_out_df_element(df_primary_with_duplicates, 'primary', 569, "ocn-primary=569 after Step 7")
        print_out_df_element(df_primary_with_duplicates, 'primary', 51451923, "ocn-primary=51451923 after Step 7")

    df = subsetOCNWithMultipleCIDs(analysis_df, df_primary_with_duplicates)
    del analysis_df
    del df_primary_with_duplicates

    if debug_mode:
        print_out_df_info_and_head(df)
        print_out_df_element(df, 'primary', 569, "ocn-primary=569 after Step 8")
        print_out_df_element(df, 'primary', 51451923, "ocn-primary=51451923 after Step 8")

    print("Find deduplicate clusters")
    duplicates_df = find_duplicate_clusters(df)
    del df

    if debug_mode:
        print_out_df_info_and_head(duplicates_df)
        print_out_df_element(duplicates_df, 'primary', 569, "ocn-primary=569 after Step 11")
        print_out_df_element(duplicates_df, 'oclc', 569, "ocn=569 after Step 11")
        print_out_df_element(duplicates_df, 'primary', 51451923, "ocn-primary=51451923 after Step 11")
        print_out_df_element(duplicates_df, 'oclc', 51451923, "ocn-primary=51451923 after Step 11")
        print_out_df_element(duplicates_df, 'oclc', 1335344, "ocn=1335344 after Step 11")

    print("Step 12 - get htid/autoid for cids with higher cid values")
    autoids_df = duplicates_df[["z_record_autoid"]].sort_values(by=['z_record_autoid'])
    autoids_df = autoids_df.drop_duplicates()

    del duplicates_df

    if debug_mode:
        print_out_df_info_and_head(autoids_df)

    zephir_ids_df = pd.read_csv(zephir_items_file, header=0, usecols=[3, 4], names=["htid", "z_record_autoid"], dtype={"htid":object, "z_record_autoid":int}, error_bad_lines=False)
    zephir_ids_df = zephir_ids_df.sort_values(by=['z_record_autoid', 'htid'])
    zephir_ids_df = zephir_ids_df.drop_duplicates()

    df = autoids_df.merge(zephir_ids_df, on='z_record_autoid')
    if debug_mode:
        print_out_df_info_and_head(df)

    print("Save autoid and htids to file ...")
    output_filename = os.path.splitext(output_marc_file)[0]
    id_file = output_filename + "_ids.txt"
    df.to_csv(id_file, index=False, header=False)
    print("The autoid and htids are saved in {}".format(id_file))

    del zephir_ids_df
    del df

    print("Output Zephir records for reload")
    batch_size = 10000
    output_xmlrecords_in_batch(autoids_df, output_filename, db_connect_str, batch_size)
    print("Records for reload are saved in file: {}".format(output_marc_file))


def cleanupData(zephir_item_detail):
    # Step 5 - CLEANUP DATA
    # coerce identifier data from objects, to numeric
    zephir_item_detail["oclc"] = zephir_item_detail["oclc"].apply(lambda x: int(x) if str(x).isdigit() else None)
    zephir_item_detail["oclc"] = zephir_item_detail["oclc"].apply(pd.to_numeric, errors='coerce')

    # drop null rows
    zephir_item_detail = zephir_item_detail.dropna()

    # cast data as integers (the "to_numberic" causes change in type) - drops leading zeros
    zephir_item_detail["oclc"] = zephir_item_detail["oclc"].astype('int')

    return zephir_item_detail

def createAnalysisDataframe(zephir_concordance_df, zephir_item_detail):
    print("# Step 6 - Analysis - join DFs by oclc")
    # Create analysis table by joining with zephir concordance
    # this results in a zephir table with both the original oclc and the primary oclc
    analysis_df = zephir_item_detail.merge(zephir_concordance_df, on='oclc', how='left')
    # drop lines with primay=NA 
    analysis_df = analysis_df.dropna()
    analysis_df["oclc"] = analysis_df["oclc"].astype('int')
    analysis_df["primary"] = analysis_df["primary"].astype('int')

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

    return df

def subsetOCNWithMultipleCIDs(analysis_df, df_primary_with_duplicates):
    print("Step 8 - create a subset of analysis data with only primary numbers that have >1 CID assoicated using a join")
    df = analysis_df.dropna().merge(df_primary_with_duplicates, on='primary', how='right')

    print("ocn-primary=569 step 8 - before sort")
    print(df.loc[df['primary'] == 569])

    print("ocn-primary=51451923 step 8 - before sort")
    print(df.loc[df['primary'] == 51451923])

    df = df.sort_values(by=['primary', 'cid'])
    
    print("ocn-primary=569 step 8 - after sort")
    print(df.loc[df['primary'] == 569])

    print("ocn-primary=51451923 step 8 - after sort")
    print(df.loc[df['primary'] == 51451923])

    return df

def find_duplicate_clusters(df):
    """Duplicate clusters are the ones share the same primary OCN with another cluster (same primary OCN with different CIDs).
    This function identifies clusters which has duplicates, marks the one with the lowest CID as base cluster and other clusters with higher CID as duplicates, then returns a new dataframe with only the duplicates. 
    """
    print("Step 10 - create lookup table for the lowest CID per primary number")
    # Step 10 - create lookup table for the lowest CID per primary number 
    lowest_cid_df = df[~df.duplicated(subset=['primary'],keep='first')][["primary","cid"]]

    if debug_mode:
        print_out_df_info_and_head(lowest_cid_df)
        print_out_df_element(lowest_cid_df, 'primary', 569, "ocn-primary=569 after step 10")
        print_out_df_element(lowest_cid_df, 'primary', 51451923, "ocn-primary=51451923 after step 10")

    lowest_cid_df = dict(zip(lowest_cid_df.primary, lowest_cid_df.cid))

    # preserve rows where the cid is higher than the first cid matching the OCLC
    dups = []
    for i, row in df.iterrows():
        dups.append(row['cid'] > lowest_cid_df[row["primary"]])

    print("Step 11 - create a dataframe subset of all the duplicate CID-HTIDs")
    # Step 11 - create a dataframe subset of all the duplicate CID-HTIDs
    # Note: Some duplicate CIDs may have additional records with a different OCLC
    higher_cid_duplicates_df = df[dups]

    if debug_mode:
        print_out_df_info_and_head(higher_cid_duplicates_df)

    return higher_cid_duplicates_df


def print_out_df_element(df, column, value, msg=None):
    if msg is not None:
        print(msg)
    print(df.loc[df[column] == value])

def print_out_df_info_and_head(df, head_lines=None):
    print(df.info())
    if head_lines is not None:
        print(df.head(head_lines))
    else:
        print(df.head())

if __name__ == '__main__':
    main()
