#!/usr/bin/env python

import datetime
import os
import socket
import zlib

import click
from environs import Env
import mysql.connector
from sqlalchemy import create_engine

from export_cache import ExportCache
from vufind_formatter import VufindFormatter
from lib.new_utils import ConsoleMessenger
import lib.new_utils as utils


def ht_bib_incr(
    console=None,
    merge_version=None,
    use_cache=None,
    quiet=False,
    verbose=True,
    force=False,
):
    # APPLICATION SETUP
    # load environment
    env = Env()
    env.read_env()

    # Print handler to manage when and how messages should print
    if not console:
        console = ConsoleMessenger(quiet, verbose)

    ROOT_PATH = os.environ.get("ZEPHIR_ROOT_PATH") or os.path.join(
        os.path.dirname(__file__), ".."
    )
    ENV = os.environ.get("ZEPHIR_ENV")
    CONFIG_PATH = os.environ.get("ZEPHIR_CONFIG_PATH") or os.path.join(
        ROOT_PATH, "config"
    )
    OVERRIDE_CONFIG_PATH = os.environ.get("ZEPHIR_OVERRIDE_CONFIG_PATH")
    CACHE_PATH = os.environ.get("ZEPHIR_CACHE_PATH") or os.path.join(ROOT_PATH, "cache")
    EXPORT_PATH = os.environ.get("ZEPHIR_EXPORT_PATH") or os.path.join(
        ROOT_PATH, "export"
    )

    # load all configuration files in directory
    config = utils.load_config(CONFIG_PATH)

    # used in testing, config files in test data will override local config files
    if OVERRIDE_CONFIG_PATH is not None and os.path.isdir(OVERRIDE_CONFIG_PATH):
        config = utils.load_config(OVERRIDE_CONFIG_PATH, config)

    export_filename = "ht_bib_export_incr_{}.json".format(
        datetime.datetime.today().strftime("%Y-%m-%d")
    )

    db = config.get("database", {}).get(ENV)

    today_date = datetime.date.today().strftime("%Y-%m-%d")
    tomorrow_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    cid_stmt = "select distinct cid from zephir_records where attr_ingest_date is not null and last_updated_at between '{}' and '{}' order by cid".format(
        today_date, tomorrow_date
    )
    console.debug(cid_stmt)
    start_time = datetime.datetime.now()

    try:
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
        cursor.execute(cid_stmt)

        engine = create_engine(
            "sqlite:///{}/cache-{}-{}.db".format(CACHE_PATH, merge_version, today_date),
            echo=False,
        )

        export_filepath = os.path.join(EXPORT_PATH, export_filename)

        with open((export_filepath), "a") as export_file, engine.connect() as con:
            for idx, cid_row in enumerate(cursor):
                get_cache_stmt = "select cache_data from cache where cache_id = '{}'".format(
                    cid_row[0]
                )
                result = con.execute(get_cache_stmt)
                for idx, cache_row in enumerate(result):
                    export_file.write(
                        zlib.decompress(cache_row[0]).decode("utf8") + "\n"
                    )
        console.debug(
            "Finished: {} (Elapsed: {})".format(
                merge_version, str(datetime.datetime.now() - start_time)
            )
        )
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    create_bib_export()
