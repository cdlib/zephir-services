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
from tact_db_utils import find_last_edit_timestamp
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

    start_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    print(start_timestamp)


    results = find_last_edit_timestamp(database)
    print(results)
    last_edit_1 = None
    if results:
        last_edit_1 = results[0]['last_edit']

    print("last edit: {}".format(last_edit_1))

    #results = find_tact_publisher_reports_by_id(database, 1)
    results = find_tact_publisher_reports_by_publisher(database, "ACM")
    print("read publisher=ACM")
    print(results)
    for result in results:
        print("{},{}\n".format(result['id'], result['doi']))


    results = find_last_edit_timestamp(database)
    print(results)
    if results:
        last_edit_1 = results[0]['last_edit']

    print("last edit: {}".format(last_edit_1))


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
    insert_tact_publisher_reports(database, publisher_report_list_1)

    update_transaction_logs(database, start_timestamp)

    print("write multiple values")
    print(publisher_report_list_2)
    insert_tact_publisher_reports(database, publisher_report_list_2)

    update_transaction_logs(database, start_timestamp)

    results = find_last_edit_timestamp(database)
    print(results)
    last_edit_2 = None
    for result in results: 
        last_edit_2 = result['last_edit']

    print("last edit: {}".format(last_edit_2))

def update_transaction_logs(database, start_timestamp):
    results = find_new_records(database, start_timestamp)
    print("new")
    print(results)
    transactions = []
    for record in results:
        del record['id']
        del record['create_date']
        del record['last_edit']
        record['transaction_status'] = 'N'
        transactions.append(record)

    if transactions:
        insert_tact_transaction_log(database, transactions)


    results = find_updated_records(database, start_timestamp)
    print("updates")
    print(results)

    transactions = []
    for record in results:
        del record['id']
        del record['create_date']
        del record['last_edit']
        record['transaction_status'] = 'U'
        transactions.append(record)

    if transactions:
        insert_tact_transaction_log(database, transactions)


if __name__ == '__main__':
    test_read_write_tact_records()
