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
    orig_statement = "select cid, db_updated_at, zr.id as htid, var_score, concat(cid,'_',zr.autoid) as vufind_sort, metadata_json from zephir_records as zr inner join zephir_filedata as zf on zr.id = zf.id and attr_ingest_date is not null order by cid, var_score, vufind_sort limit 30"

    # original_statement = "select cid, zr.id as htid, var_score, concat(cid,'_',zr.autoid) as vufind_sort from zephir_records as zr where attr_ingest_date is not null order by cid, var_score, vufind_sort"
    # all_statement = "select 1"
    start_time = datetime.datetime.time(datetime.datetime.now())


    with open("export.json", "a") as export_file:

        engine = create_engine(
            "sqlite:///cache/complete-gz.db",
            echo=False,
        )
        with engine.connect() as con:
            create_table_stmt = "select cache_data from cache"
            result = con.execute(create_table_stmt)
            for idx, row in enumerate(result):
                export_file.write(zlib.decompress(row[0]).decode('utf8')+"\n")

        print("start:{}".format(start_time))
        print(datetime.datetime.time(datetime.datetime.now()))


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
