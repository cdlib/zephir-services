#!/usr/bin/env python

import datetime
import os
import zlib

import mysql.connector
from sqlalchemy import create_engine

import lib.utils as utils


def ht_bib_incr(
    console, cache_path=None, output_path=None, merge_version=None, force=False
):
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
    console.debug("Started: {}".format(debug_start_time))

    # LOAD: environment, configuration
    default_root_dir = os.path.join(os.path.dirname(__file__), "..")
    APP = utils.AppEnv(name="ZEPHIR", root_dir=default_root_dir)
    console.debug("Loading application environment and configuration")
    console.debug("Environment: {}".format(APP.ENV))
    console.debug("Configuration: {}".format(APP.CONFIG_PATH))
    count = 0
    try:
        # DATABASE: Prepare connection and statements
        # get database connection
        db = APP.CONFIG.get("database", {}).get(APP.ENV)
        db_config = utils.DatabaseHelper(config=db, env_prefix="ZEPHIR")
        conn_args = db_config.connection_args()
        db_connection = mysql.connector.connect(**conn_args)
        # prepare sql statement
        today_date = datetime.date.today().strftime("%Y-%m-%d")
        tomorrow_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
        SQL_STMT = "select distinct cid from zephir_records where attr_ingest_date is not null and last_updated_at between '{}' and '{}' order by cid".format(
            today_date, tomorrow_date
        )
        # execute query, to be iterated below
        db_cursor = db_connection.cursor()
        db_cursor.execute(SQL_STMT)

        # CACHE: What cache should be used?
        cache_path = cache_path or APP.CACHE_PATH
        # use working directory if relative path given
        if not os.path.isabs(cache_path):
            cache_path = os.path.join(os.getcwd(), cache_path)
        if not os.path.exists(cache_path):
            console.error("Cache path invalid")
            raise SystemExit(2)
        if os.path.isdir(cache_path):
            console.debug("Cache file not given. Using cache template")
            cache_template = "cache-{}-{}.db".format(
                merge_version, datetime.datetime.today().strftime("%Y-%m-%d")
            )
            console.debug("Cache template:{}".format(cache_template))
            cache_path = "{}/{}".format(cache_path, cache_template)
        console.debug("Using Cache: {}".format(cache_path))
        cache_url = "sqlite:///{}".format(cache_path)
        cache = create_engine(cache_url, echo=False)

        # OUTPUT: Where should the results go?
        output_path = output_path or APP.OUTPUT_PATH
        # use working directory if relative path given
        if not os.path.isabs(output_path):
            output_path = os.path.join(os.getcwd(), output_path)
        if os.path.isfile(output_path):
            if force:
                console.debug("File exists. Force option provided. Removing file")
                os.remove(output_path)
            else:
                console.error("File exists. Please remove file or use force option")
                raise SystemExit(2)
        if os.path.isdir(output_path):
            console.debug("File not given. Using filename template")
            file_template = "ht_bib_export_incr_{}.json".format(
                datetime.datetime.today().strftime("%Y-%m-%d")
            )
            output_path = os.path.join(output_path, file_template)
        console.debug("Using Output: {}".format(output_path))

        with open((output_path), "a") as export_file, cache.connect() as con:
            for idx, cid_row in enumerate(db_cursor):
                get_cache_stmt = "select cache_data from cache where cache_id = '{}'".format(
                    cid_row[0]
                )
                cache_result = con.execute(get_cache_stmt)
                for idx, cache_row in enumerate(cache_result):
                    export_file.write(
                        zlib.decompress(cache_row[0]).decode("utf8") + "\n"
                    )
                    count += 1
    finally:
        db_cursor.close()
        db_connection.close()

    console.debug(
        "Completed: {}".format(str(datetime.datetime.now() - debug_start_time))
    )
    console.info(
        "All done! Created ht-bib-incr({}) export with {} records".format(
            merge_version, count
        )
    )
    return output_path


if __name__ == "__main__":
    ht_bib_incr()
