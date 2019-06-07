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

    # APPLICATION SETUP
    # load environment
    env = Env()
    env.read_env()

    # Print handler to manage when and how messages should print
    if not console:
        console = ConsoleMessenger(quiet, verbose, very_verbose)

    ROOT_PATH = os.environ.get("ZEPHIR_ROOT_PATH") or os.path.join(
        os.path.dirname(__file__), ".."
    )
    ENV = os.environ.get("ZEPHIR_ENV")
    CONFIG_PATH = os.environ.get("ZEPHIR_CONFIG_PATH") or os.path.join(
        ROOT_PATH, "config"
    )
    OVERRIDE_CONFIG_PATH = os.environ.get("ZEPHIR_OVERRIDE_CONFIG_PATH")
    CACHE_PATH = os.environ.get("ZEPHIR_CACHE_PATH") or os.path.join(ROOT_PATH, "cache")

    # load all configuration files in directory
    config = utils.load_config(CONFIG_PATH)

    # used in testing, config files in test data will override local config files
    if OVERRIDE_CONFIG_PATH is not None:
        config = utils.load_config(OVERRIDE_CONFIG_PATH, config)

    db = config.get("database", {}).get(ENV)

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

    cache_file = os.path.join(
        CACHE_PATH,
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
        cache = ExportCache(CACHE_PATH, tmp_cache_name, force)

        console.debug(
            "Started: {} (Elapsed: {})".format(
                merge_version, str(datetime.datetime.now() - start_time)
            )
        )
        try:
            bulk_session = cache.session()
            conn_args = {
                "user": db.get("username", None),
                "password": db.get("password", None),
                "host": db.get("host", None),
                "database": db.get("database", None),
                "unix_socket": None,
            }

            socket = os.environ.get("ZEPHIR_DB_SOCKET") or config.get("socket")

            if socket:
                conn_args["unix_socket"] = socket

            conn = mysql.connector.connect(**conn_args)

            cursor = conn.cursor()
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
                os.path.join(CACHE_PATH, "{}.db".format(tmp_cache_name)), cache_file
            )
        finally:
            # TODO(cc): This will fail if cursor not defined
            cursor.close()
            conn.close()

        console.debug(
            "Finished: {} (Elapsed: {})".format(
                merge_version, str(datetime.datetime.now() - start_time)
            )
        )

    return cache_file


if __name__ == "__main__":
    create_cache()
