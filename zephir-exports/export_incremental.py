#!/usr/bin/env python

import datetime
import os
import socket
import zlib

import argparse
from environs import Env
import mysql.connector
from sqlalchemy import create_engine

from lib.utils import zephir_config

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
        "-s",
        "--selection",
        action="store",
        default="ref",
        help="Selection algorithm used for export",
    )
    args = parser.parse_args()
    selection = args.selection

    htmm_db = config["database"][config["env"]]

    today_date = datetime.date.today().strftime("%Y-%m-%d")
    tomorrow_date = (datetime.date.today() + datetime.timedelta(1)).strftime("%Y-%m-%d")
    cid_stmt = "select distinct cid from zephir_records where last_updated_at between '{}' and '{}' order by cid".format(
        today_date, tomorrow_date
    )
    print(cid_stmt)
    start_time = datetime.datetime.now()

    try:
        conn = mysql.connector.connect(
            user=htmm_db.get("username", None),
            password=htmm_db.get("password", None),
            host=htmm_db.get("host", None),
            database=htmm_db.get("database", None),
        )

        cursor = conn.cursor()
        cursor.execute(cid_stmt)

        engine = create_engine(
            "sqlite:///{}/cache-{}-{}.db".format(
                os.path.join(os.path.dirname(__file__), "cache"), selection, today_date
            ),
            echo=False,
        )
        export_filepath = os.path.join(
            os.path.dirname(__file__),
            "export/new-{}-vufind_incremental-{}.json".format(selection, today_date),
        )

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
