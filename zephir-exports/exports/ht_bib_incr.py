#!/usr/bin/env python

import datetime
import os
import socket
import zlib

import click
from environs import Env
import mysql.connector
from sqlalchemy import create_engine

from lib.export_cache import ExportCache
from lib.vufind_formatter import VufindFormatter
from lib.utils import ConsoleMessenger
import lib.utils as utils


def ht_bib_incr(console, cache_path=None, output_path=None, merge_version=None, force=False):
    """ HathiTrust Bibliographic Export (FULL) method. This method is dependent on the
    cli calling script. It selects all the entries from a cache and exports them
    into a file.

    Args:
        console: A console messenger for output provided by the callign CLI.
        cache_path: File or directory location of cache file.
        output_path: File or directory for generated file.
        merge_version: Version of merge algorithm used for cache.
        force: Boolean for overwriting existing files.

    Returns:
        The location of the generated file.

        """

    debug_start_time = datetime.datetime.now()
    # console.debug("Started: {}".format(debug_start_time))

    # LOAD: environment, configuration
    default_root_dir = os.path.join(os.path.dirname(__file__), "..")
    APP = utils.AppEnv(name="ZEPHIR", root_dir=default_root_dir)
    # console.debug("Loading application environment and configuration")
    # console.debug("Environment: {}".format(APP.ENV))
    # console.debug("Configuration: {}".format(APP.CONFIG_PATH))
    export_filename = "ht_bib_export_incr_{}.json".format(
        datetime.datetime.today().strftime("%Y-%m-%d")
    )


    # DATABASE: Prepare connection and statements
    try:

        # get database connection
        db = APP.CONFIG.get("database", {}).get(APP.ENV)
        db_config = utils.DatabaseHelper(config=db, env_prefix="ZEPHIR")
        conn_args = db_config.connection_args()
        conn = mysql.connector.connect(**conn_args)

        # prepare sql statement
        today_date = datetime.date.today().strftime("%Y-%m-%d")
        tomorrow_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
        SQL_STMT = "select distinct cid from zephir_records where attr_ingest_date is not null and last_updated_at between '{}' and '{}' order by cid".format(
            today_date, tomorrow_date
        )

        # execute query
        cursor = conn.cursor()
        cursor.execute(SQL_STMT)

        # open export cache file
        engine = create_engine(
            "sqlite:///{}/cache-{}-{}.db".format(
                APP.CACHE_PATH, merge_version, today_date
            ),
            echo=False,
        )

        export_filepath = os.path.join(APP.OUTPUT_PATH, export_filename)

        count = 0
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
                    count += 1
    finally:
        cursor.close()
        conn.close()

    console.debug(
        "Completed: {}".format(
            str(datetime.datetime.now() - debug_start_time)
        )
    )

    console.info("💫 📝  All done! Created ht-bib-incr({}) export with {} records".format(merge_version, count))

    return output_path

if __name__ == "__main__":
    create_bib_export()
