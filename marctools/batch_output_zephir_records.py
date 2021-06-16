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

from zephir_db_utils import find_marcxml_records_by_autoid_range
from zephir_db_utils import find_marcxml_records_by_autoid_list
from zephir_db_utils import find_marcxml_records_by_autoid_list_2

def test():

    env="stg"
    configs= get_configs_by_filename('config', 'zephir_db')
    db_connect_str = str(utils.db_connect_url(configs[env]))

    # sorted autoids 
    autoid_file = "./output/test_ids.txt"
    autoids_df = pd.read_csv(autoid_file, usecols=[0], names=['z_record_autoid'], header=None)

    autoids_df = autoids_df.sort_values(by=['z_record_autoid'])
    autoids_df.info()
    print(autoids_df.head(21))
    print(len(autoids_df.index))

    output_marc_file = "output/marc_file_batch_test"
    output_xmlrecords_in_batch(autoids_df, output_marc_file, db_connect_str)


def output_xmlrecords_in_batch(df, output_filename, db_connect_str, batch_size):

    autoid_list = []
    for index in df.index:
        autoid = df['z_record_autoid'][index].item()

        if (index % batch_size == 0 and index !=0):
            filename = "{}_{}.xml".format(output_filename, index)
            outfile = open(filename, 'w')
            outfile.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
            outfile.write("<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");

            records = find_marcxml_records_by_autoid_list_2(db_connect_str, autoid_list)
            for record in records:
                marcxml = re.sub("<\?xml version=\"1.0\" encoding=\"UTF-8\"\?>\n", "", record["metadata"])
                marcxml = re.sub(" xmlns=\"http://www.loc.gov/MARC21/slim\"", "", marcxml)
                outfile.write(marcxml)

            outfile.write("</collection>\n")
            outfile.close()

            autoid_list = []
        else:
            autoid_list.append(autoid)


def output_xmlrecords_in_batch_by_range(autoids_df, output_filename, db_connect_str, n_rows=10000):

    # subset of every n rows
    df1 = autoids_df.iloc[::n_rows]
    df1.info()

    print(df1.head(10))
    print(len(df1.index))

    df2 = autoids_df.iloc[[-1]]
    df2.info()
    print(df2.head())

    df = pd.concat([df1, df2], ignore_index=True)

    df.info()
    print(df.head(10))
    print(len(df.index))

    autoid_from = 0
    autoid_to = 0
    for index in df.index:
        autoid_from = autoid_to + 1
        autoid_to = df['z_record_autoid'][index].item()

        if autoid_from < autoid_to:
            filename = "{}_{}.xml".format(output_filename, index)
            outfile = open(filename, 'w')
            outfile.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
            outfile.write("<collection xmlns=\"http://www.loc.gov/MARC21/slim\">\n");

            print("from: {} , to: {}".format(autoid_from, autoid_to))
            records = find_marcxml_records_by_autoid_range(db_connect_str, autoid_from, autoid_to)
            for record in records:
                if record["z_record_autoid"] in autoids_df.values:
                    marcxml = re.sub("<\?xml version=\"1.0\" encoding=\"UTF-8\"\?>\n", "", record["metadata"])
                    marcxml = re.sub(" xmlns=\"http://www.loc.gov/MARC21/slim\"", "", marcxml)
                    outfile.write(marcxml)

            outfile.write("</collection>\n")
            outfile.close()

if __name__ == '__main__':
    test()
