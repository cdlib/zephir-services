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

import click

import lib.utils as utils
from config import get_configs_by_filename

from zephir_db_utils import createZephirItemDetailsFileFromDB
from zephir_db_utils import find_marcxml_records_by_htid
from output_zephir_records import output_xmlrecords_by_htid

def test_zephir_search(db_connect_str):

    id = "pur1.32754075735872"
    results = find_marcxml_records_by_htid(db_connect_str, id)
    for result in results:
        print (result)

@click.command()
@click.option('-e', '--env', default="dev")
@click.option('-H', '--input-htid-file')
@click.option('-S', '--search-zephir-database', is_flag=True, help="Get Zephir items data from database.")
@click.option('-Z', '--zephir-items-file', default="./data/zephir_items.csv")
@click.option('-C', '--oclc-concordance-file', default="./data/zephir_concordance.csv")
@click.option('-o', '--output-marc-file', default="./output/marc_records_for_reload.xml")
@click.option('-c', '--output-cid-file', default="./output/cids_for_auto_split.txt")
def main(env, input_htid_file, search_zephir_database,
        zephir_items_file, oclc_concordance_file, output_marc_file, output_cid_file):
    print(env)
    print(input_htid_file)
    print(search_zephir_database)
    print(zephir_items_file)
    print(oclc_concordance_file)
    print(output_marc_file)
    print(output_cid_file)

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

    f_output_split_clusters(zephir_items_file, oclc_concordance_file, output_cid_file)

    print("CIDs for auto-split are saved in file: {}".format(output_cid_file))


def f_output_split_clusters(zephir_items_file, oclc_concordance_file, output_cid_file):
    # memory use: 543 MB
    print("")
    print("Read in data to DF: cid, oclc, contribsys_id")
    raw_zephir_item_detail = pd.read_csv(zephir_items_file, header=0, usecols=[0, 1, 2], names=["cid", "oclc", "contribsys_id"], dtype={"cid":int, "oclc":object, "contribsys_id":object}, error_bad_lines=False)

    print("raw_zephir_item_detail: before drop duplicates")
    print(raw_zephir_item_detail.info())
    print(raw_zephir_item_detail.head())

    raw_zephir_item_detail.drop_duplicates(inplace=True)
    print("raw_zephir_item_detail: after drop duplicates")
    print(raw_zephir_item_detail.info())
    print(raw_zephir_item_detail.head())

    # memory use: 724 MB
    print("")
    print("Cleanup Data")
    zephir_item_detail = cleanupData(raw_zephir_item_detail)
    del raw_zephir_item_detail

    zephir_item_detail.drop_duplicates(inplace=True)
    print("zephir_item_detail: after drop duplicates")
    print(zephir_item_detail.info())
    print(zephir_item_detail.head())

    # 150 MB
    print("")
    print("Get Concordance data")
    zephir_concordance_df = readCsvFileToDataFrame(oclc_concordance_file)
    print(zephir_concordance_df.loc[zephir_concordance_df['primary'] == 569])
    print(zephir_concordance_df.loc[zephir_concordance_df['oclc'] == 569])

    # 980 MB
    print("")
    print("Join data frames")
    analysis_df = createAnalysisDataframe(zephir_concordance_df, zephir_item_detail)
    del zephir_item_detail
    del zephir_concordance_df

    analysis_df.drop_duplicates(inplace=True)
    print("analysis_df: after drop duplicates")
    print(analysis_df.info())
    print(analysis_df.head(10))
    print(analysis_df.loc[analysis_df['cid'] == 11])

    print("")
    print("Find CIDs with multiple OCLC primary numbers - Full Collection") 
    cids_with_multi_primary_fc_df = findCIDsWithMultipleOCNs(analysis_df)

    print("")
    print("Find Contributor system IDs with multiple OCLC primary numbers")
    contribsys_ids_with_multi_primary_fc_df = findContribIDsWithWithMultipleOCNs(analysis_df)

    del analysis_df

    print("")
    print("CIDs with multiple OCLCs, but no overlapping contribsys records")
    auto_splitable_cids = subsetCIDsWithMultipleOCNs(cids_with_multi_primary_fc_df, contribsys_ids_with_multi_primary_fc_df)

    del cids_with_multi_primary_fc_df
    del contribsys_ids_with_multi_primary_fc_df

    with open(output_cid_file, "w") as output_f:
        for ind in auto_splitable_cids.index :
            output_f.write("cid=" + ("000000000" + str(auto_splitable_cids["cid"][ind]))[-9:] + "\n")


def readCsvFileToDataFrame(file_path):
    zephir_concordance_df = pd.read_csv(file_path, names=["oclc", "primary"], header=0)
    print(zephir_concordance_df.info())
    print(zephir_concordance_df.head())
    return zephir_concordance_df

def cleanupData(zephir_item_detail):
    #Step 5 - CLEANUP DATA
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
    print("## Step 6 - Analysis - join DFs by oclc")
    # Create analysis table by joining with zephir concordance
    # this results in a zephir table with both the original oclc and the primary oclc
    analysis_df = zephir_item_detail.merge(zephir_concordance_df, on='oclc', how='left')
    analysis_df["oclc"] = analysis_df["oclc"].astype('Int64')
    analysis_df["primary"] = analysis_df["primary"].astype('Int64')
    
    print(analysis_df.info())
    print(analysis_df.head())

    return analysis_df 





def findCIDsWithMultipleOCNs(analysis_df):
    print("## Step 7 - find CIDs with multiple OCNs")
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
    print("## Step 8 - Contributor system IDs with multiple OCLC primary numbers")
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
