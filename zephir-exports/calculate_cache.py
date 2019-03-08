#!/usr/bin/env python

import datetime
import os
import socket
import zlib

import argparse
from environs import Env
import json
import mysql.connector

from lib.export_cache import ExportCache
from lib.utils import zephir_config
from lib.vufind_formatter import VufindFormatter

# APPLICATION SETUP
# load environment
env = Env()
env.read_env()

# load configuration files
config = zephir_config(
    env("ZEPHIR_ENV", socket.gethostname()).lower(),
    os.path.join(os.path.dirname(__file__), "config"),
)


def main(argv=None):
    # Command line argument configuration
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s", "--selection", action="store", help="Selection algorithm used for export"
    )
    args = parser.parse_args()
    selection = args.selection
    if selection is None:
        raise "Must pass a selection algorithm to use. See --help"

    htmm_db = config["database"][config["env"]]

    # HTMM_DB_CONNECT_STR = str(
    #     URL(
    #         htmm_db.get("drivername", None),
    #         htmm_db.get("username", None),
    #         htmm_db.get("password", None),
    #         htmm_db.get("host", None),
    #         htmm_db.get("port", None),
    #         htmm_db.get("database", None),
    #     )
    # )
    # htmm_engine = create_engine(HTMM_DB_CONNECT_STR)

    sql_select = {
        "v2": "select cid, db_updated_at, metadata_json, "
        "var_usfeddoc, var_score, concat(cid,'_',zr.autoid) as vufind_sort  "
        "from zephir_records as zr "
        "inner join zephir_filedata as zf on zr.id = zf.id "
        "where attr_ingest_date is not null "
        "order by cid, var_score DESC, vufind_sort ASC",
        "v3": "select cid, db_updated_at, metadata_json, "
        "var_usfeddoc, var_score, concat(cid,'_',zr.autoid) as vufind_sort  "
        "from zephir_records as zr "
        "inner join zephir_filedata as zf on zr.id = zf.id "
        "where attr_ingest_date is not null "
        "order by cid, var_usfeddoc DESC, var_score DESC, vufind_sort ASC",
    }
    start_time = datetime.datetime.now()
    # live_index = {}
    max_date = None
    # record_count = 0
    records = []
    # htid = None
    # current_cid = None

    cache = ExportCache(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "cache")),
        "cache-{}-{}".format(selection, datetime.datetime.today().strftime("%Y-%m-%d")),
    )

    try:
        bulk_session = cache.session()
        conn = mysql.connector.connect(
            user=htmm_db.get("username", None),
            password=htmm_db.get("password", None),
            host=htmm_db.get("host", None),
            database=htmm_db.get("database", None),
        )

        cursor = conn.cursor()
        cursor.execute(sql_select[selection])

        curr_cid = None
        records = []
        entries = []
        max_date = None
        for idx, row in enumerate(cursor):
            cid, db_date, record, var_usfeddoc, var_score, vufind_sort = row
            if cid != curr_cid or curr_cid is None:
                # write last cluster
                if curr_cid:
                    cache_id = curr_cid
                    cache_data = json.dumps(
                        VufindFormatter.create_record(curr_cid, records).as_dict(),
                        separators=(",", ":"),
                    )
                    cache_key = zlib.crc32(
                        "{}{}".format(len(records), max_date).encode("utf8")
                    )
                    cache_date = max_date
                    entry = cache.entry(cache_id, cache_key, cache_data, cache_date)
                    entries.append(entry)

                # prepare next cluster
                curr_cid = cid
                records = [record]
                max_date = db_date
            else:
                if db_date > max_date:
                    max_date = db_date
                records.append(record)

            # periodic save to chunk work
            if idx % 5000 == 0:
                bulk_session.bulk_save_objects(entries)
                entries = []

        cache_id = curr_cid
        cache_data = json.dumps(
            VufindFormatter.create_record(curr_cid, records).as_dict(),
            separators=(",", ":"),
        )
        cache_key = zlib.crc32("{}{}".format(len(records), max_date).encode("utf8"))
        cache_date = max_date
        entry = cache.entry(cache_id, cache_key, cache_data, cache_date)
        entries.append(entry)
        bulk_session.bulk_save_objects(entries)
        bulk_session.commit()
        bulk_session.close()
        print(
            "Finished: {} (Elapsed: {})".format(
                selection, str(datetime.datetime.now() - start_time)
            )
        )
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
