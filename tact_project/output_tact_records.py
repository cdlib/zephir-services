import os
import sys

import environs
import re
import datetime

from sqlalchemy import create_engine
from sqlalchemy import text

import lib.utils as utils
from tact_db_utils import find_tact_transactions_by_id
from tact_db_utils import insert_tact_transactions

def test_read_write_tact_records():

    env="dev"
    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, "config")
    CONFIG_FILE = "tact_db"
    configs= utils.get_configs_by_filename(CONFIG_PATH, CONFIG_FILE)
    db_connect_str = str(utils.db_connect_url(configs[env]))

    print(db_connect_str)

    results = find_tact_transactions_by_id(db_connect_str, 1)
    print("read id=1")
    print(results)
    for result in results:
        print("{},{}\n".format(result['id'], result['doi']))

    trans_dict_list = [
            {
            'publisher': "ACM",
            'doi': "111",
            'article_title': "test title 1",
            'uc_institution': "UCD",
            },
            {
            'publisher': "ACM",
            'doi': "222",
            'article_title': "test title 2",
            'uc_institution': "UCD",
            },
            {
            'publisher': "123456789022345678903234567890",
            'doi': "3",
            'article_title': "test title 3 - publisher exceed max length",
            'uc_institution': "UCD",
            },
        ]

    trans_dict = {
            'publisher': "ACM",
            'doi': "111",
            'article_title': "test title 1",
            'uc_institution': "UCD",
            }

    print("write one value")
    insert_tact_transactions(db_connect_str, trans_dict)

    print("write multiple values")
    insert_tact_transactions(db_connect_str, trans_dict_list)

if __name__ == '__main__':
    test_read_write_tact_records()
