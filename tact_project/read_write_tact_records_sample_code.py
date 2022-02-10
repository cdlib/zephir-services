import os
import sys

import environs
import re
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy import text

import lib.utils as utils
from tact_db_utils import find_tact_publisher_reports_by_id
from tact_db_utils import find_tact_publisher_reports_by_publisher
from tact_db_utils import insert_tact_publisher_reports
from tact_db_utils import insert_tact_transaction_log
from tact_db_utils import find_last_edit_by_doi
from tact_db_utils import init_database
from tact_db_utils import find_new_records
from tact_db_utils import find_updated_records

def test_read_write_tact_records():

    env="dev"
    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, "config")
    CONFIG_FILE = "tact_db"
    configs= utils.get_configs_by_filename(CONFIG_PATH, CONFIG_FILE)
    db_conn_str = str(utils.db_connect_url(configs[env]))

    print(db_conn_str)
    database = init_database(db_conn_str)

    #results = find_tact_publisher_reports_by_id(database, 1)
    results = find_tact_publisher_reports_by_publisher(database, "ACM")
    print("read publisher=ACM")
    print(results)
    for result in results:
        print("{},{}\n".format(result['id'], result['doi']))


    publisher_report_list_1 = [{
            'publisher': "ACM",
            'doi': "111",
            'article_title': "test title 1",
            'uc_institution': "UCD",
            }
            ]

    publisher_report_list_2 = [
            {
            'publisher': "ACM",
            'doi': "111",
            'article_title': "test title 1 updated",
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

    print("write one value")
    print(publisher_report_list_1)
    for record in publisher_report_list_1:
        update_database(database, record)

    print("write multiple values")
    print(publisher_report_list_2)
    # you can insert multiple records in one call
    #insert_tact_publisher_reports(database, publisher_report_list_2)

    for record in publisher_report_list_2:
        update_database(database, record)


def update_database(database, record):
    last_edit_1 = None
    last_edit_2 = None
    results = find_last_edit_by_doi(database, record['doi'])
    print(results)
    if results:
        last_edit_1 = results[0]['last_edit']

    insert_tact_publisher_reports(database, [record])

    results = find_last_edit_by_doi(database, record['doi'])

    print(results)
    if results:
        last_edit_2 = results[0]['last_edit']

    if last_edit_1 is None:
        if last_edit_2:
            print("new record")
            record['transaction_status'] = 'N'
    else:
        if last_edit_2 > last_edit_1:
            print("Updated record")
            record['transaction_status'] = 'U'

    if 'transaction_status' in record.keys():
        insert_tact_transaction_log(database, [record])

if __name__ == '__main__':
    test_read_write_tact_records()
