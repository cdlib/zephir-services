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
from lib.utils import ConsoleMessenger
import lib.utils as utils


def ht_bib_incr(
    console=None,
    merge_version=None,
    use_cache=None,
    quiet=False,
    verbose=True,
    very_verbose=False,
    force=False,
):

    # Print handler to manage when and how messages should print
    if not console:
        console = ConsoleMessenger(quiet, verbose, very_verbose)

    # Load application environment, configuration
    default_root_dir = os.path.join(os.path.dirname(__file__), "..")
    APP = utils.AppEnv(name="ZEPHIR", root_dir=default_root_dir)

    export_filename = "ht_bib_export_incr_{}.json".format(
        datetime.datetime.today().strftime("%Y-%m-%d")
    )

    db = APP.CONFIG.get("database", {}).get(APP.ENV)

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
        db_config = utils.DatabaseConfig(config=db, env_prefix="ZEPHIR")
        conn_args = db_config.connection_args()
        conn = mysql.connector.connect(**conn_args)

        cursor = conn.cursor()
        cursor.execute(cid_stmt)

        engine = create_engine(
            "sqlite:///{}/cache-{}-{}.db".format(
                APP.CACHE_PATH, merge_version, today_date
            ),
            echo=False,
        )

        export_filepath = os.path.join(APP.EXPORT_PATH, export_filename)

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
