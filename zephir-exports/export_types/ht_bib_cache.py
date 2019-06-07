#!/usr/bin/env python

import datetime
import os
import socket
import zlib

import click
from environs import Env
import json
import mysql.connector

from export_cache import ExportCache
from vufind_formatter import VufindFormatter
from lib.utils import ConsoleMessenger
import lib.utils as utils


def ht_bib_cache(
    console=None,
    merge_version=None,
    quiet=False,
    verbose=True,
    very_verbose=False,
    force=False,
):

    # Initilize for output and errors
    if not console:
        console = ConsoleMessenger(quiet, verbose, very_verbose)

    # Load application environment, configuration
    default_root_dir = os.path.join(os.path.dirname(__file__), "..")
    APP = utils.AppEnv(name="ZEPHIR",root_dir=default_root_dir)

    debug_start_time = datetime.datetime.now()
    console.debug("Starting to build cache: {}".format(debug_start_time))

    cache_file = os.path.join(
        APP.CACHE_PATH,
        "cache-{}-{}.db".format(
            merge_version, datetime.datetime.today().strftime("%Y-%m-%d")
        ),
    )

    # remove file if force overwrite specified
    if force and os.path.isfile(cache_file):
        console.debug("Forced; removing existing cache.")
        os.remove(cache_file)

    if os.path.isfile(cache_file):
        console.debug("Skipping; cache file exists. Force to overwrite.")
    else:
        console.debug("Creating new cache file.")
        tmp_cache_name = "tmp-cache-{}-{}".format(
            merge_version, datetime.datetime.today().strftime("%Y-%m-%d_%H%M%S.%f")
        )
        cache = ExportCache(APP.CACHE_PATH, tmp_cache_name, force)

        console.debug(
            "Started: {} (Elapsed: {})".format(
                merge_version, str(datetime.datetime.now() - debug_start_time)
            )
        )
        try:
            bulk_session = cache.session()

            # Load database settings
            db_settings = APP.CONFIG.get("database", {}).get(APP.ENV)
            db_config = utils.DatabaseConfig(config=db_settings, env_prefix="ZEPHIR")
            conn_args = db_config.connection_args()

            conn = mysql.connector.connect(**conn_args)
            cursor = conn.cursor()


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
            cursor.execute(sql_select[merge_version])

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
            os.rename(
                os.path.join(APP.CACHE_PATH, "{}.db".format(tmp_cache_name)), cache_file
            )
        finally:
            # TODO(cc): This will fail if cursor not defined
            cursor.close()
            conn.close()

        console.debug(
            "Finished: {} (Elapsed: {})".format(
                merge_version, str(datetime.datetime.now() - debug_start_time)
            )
        )

    return cache_file


if __name__ == "__main__":
    create_cache()
