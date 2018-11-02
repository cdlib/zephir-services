#!/usr/bin/env python

import datetime
import os
import socket
import zlib

from environs import Env
import json
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import yaml

from lib.export_cache import ExportCache
from lib.utils  import zephir_config
from lib.vufind_formatter import VufindFormatter

print(datetime.datetime.time(datetime.datetime.now()))

# APPLICATION SETUP
# load environment
env = Env()
env.read_env()

# load configuration files
config = zephir_config(env("ZEPHIR_ENV", socket.gethostname()).lower(), os.path.join(os.path.dirname(__file__),"config"))


def main():
    htmm_db = config["database"][config["env"]]

    HTMM_DB_CONNECT_STR = str(
        URL(
            htmm_db.get("drivername", None),
            htmm_db.get("username", None),
            htmm_db.get("password", None),
            htmm_db.get("host", None),
            htmm_db.get("port", None),
            htmm_db.get("database", None),
        )
    )
    htmm_engine = create_engine(HTMM_DB_CONNECT_STR)
    # live_statement = "select cid, db_updated_at, zr.id as htid, var_usfeddoc, var_score, metadata_json from zephir_records as zr inner join zephir_filedata as zf on zr.id = zf.id and attr_ingest_date is not null and cid = \"{}\" order by var_usfeddoc, var_score"
    #feddoc_statement = "select cid, db_updated_at, zr.id as htid, var_usfeddoc, var_score, metadata_json from zephir_records as zr inner join zephir_filedata as zf on zr.id = zf.id and attr_ingest_date is not null order by cid, var_usfeddoc, var_score limit 30"
    orig_stmt = "select cid, db_updated_at, zr.id as htid, var_score, concat(cid,'_',zr.autoid) as vufind_sort, metadata_json from zephir_records as zr inner join zephir_filedata as zf on zr.id = zf.id where attr_ingest_date is not null order by cid, var_score, vufind_sort"
    start_time = datetime.datetime.time(datetime.datetime.now())
    live_index = {}
    max_date = None
    record_count = 0
    records = []
    htid = None
    current_cid = None

    cache = ExportCache(os.path.abspath("cache"),'complete-gz')

    try:
        bulk_session = cache.session()
        conn = mysql.connector.connect(
            user=htmm_db.get("username", None),
            password=htmm_db.get("password", None),
            host=htmm_db.get("host", None),
            database=htmm_db.get("database", None))

        cursor = conn.cursor()
        cursor.execute(orig_stmt)

        curr_cid = None
        htid = None
        records = None
        entries = []
        max_date = None
        for idx, row in enumerate(cursor):
            cid, db_date, htid, var_score, vufind_sort, record = row
            if cid != curr_cid or curr_cid is None:
                # write last cluster
                if curr_cid:
                    cache_id = curr_cid
                    cache_data = json.dumps(VufindFormatter.create_record(curr_cid, records).as_dict(),separators=(',',':'))
                    cache_key = (zlib.crc32("{}{}".format(len(records), max_date).encode('utf8')))
                    cache_date = max_date
                    entry = cache.entry(cache_id, cache_key, cache_data, cache_date)
                    entries.append(entry)
                    if idx % 5000 == 0:
                        bulk_session.bulk_save_objects(entries)
                        entries = []
                    print("{}: {}".format(cache_id, cache_key))
                # prepare next cluster
                curr_cid = cid
                records = [record]
                max_date = db_date
            else:
                if db_date > max_date:
                    max_date = db_date
                records.append(record)
                
        cache_id = curr_cid
        cache_data = json.dumps(VufindFormatter.create_record(curr_cid, records).as_dict(),separators=(',',':'))
        cache_key = (zlib.crc32("{}{}".format(len(records), max_date).encode('utf8')))
        cache_date = max_date
        entry = cache.entry(cache_id, cache_key, cache_data, cache_date)
        entries.append(entry)
        bulk_session.bulk_save_objects(entries)
        bulk_session.commit()
        bulk_session.close()
        print("start:{}".format(start_time))
        print(datetime.datetime.time(datetime.datetime.now()))
    finally:
        cursor.close()
        conn.close()

        # cache_key = (zlib.crc32("{}{}".format(record_count, max_date).encode()))
        # record = VufindFormatter.create_record("000000001", htid, records)
        # json.dumps(record.as_dict(), separators=(',',':'))

###############################################
    # with htmm_engine.connect() as con1:
    #     cids_result = con1.execute("select distinct(cid) as cid from zephir_records where attr_ingest_date is not null limit 100")
    #     print(datetime.datetime.time(datetime.datetime.now()))
    #     for cid in cids_result:
    #         with htmm_engine.connect() as con2:
    #             result = con2.execute(live_statement.format(cid.cid))
    #             for idx, row in enumerate(result):
    #                 if htid is None:
    #                     htid = row.htid
    #                 if max_date is None or row.db_updated_at > max_date:
    #                     max_date = row.db_updated_at
    #                 record_count = idx + 1
    #                 records.append(row.metadata_json)
    #
    #
    #         cache_key = (zlib.crc32("{}{}".format(record_count, max_date).encode()))
    #         # print(records)
    #         record = VufindFormatter.create_record("000000001", htid, records)
    #         json.dumps(record.as_dict(), separators=(',',':'))
    # print(datetime.datetime.time(datetime.datetime.now()))

#####################################################################
if __name__ == "__main__":
    main()
