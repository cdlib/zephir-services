#!/usr/bin/env python

import datetime
import os
import socket
import zlib

import argparse
from environs import Env
import json
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import yaml

from lib.export_cache import ExportCache
from lib.utils import zephir_config
from lib.vufind_formatter import VufindFormatter

print(datetime.datetime.time(datetime.datetime.now()))

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
        default="og",
        help="Selection algorithm used for export",
    )
    args = parser.parse_args()
    selection = args.selection

    start_time = datetime.datetime.time(datetime.datetime.now())

    with open(
        "export-{}-{}.json".format(
            selection, datetime.datetime.today().strftime("%Y-%m-%d")
        ),
        "a",
    ) as export_file:

        engine = create_engine(
            "sqlite:///cache/cache-{}-{}.db".format(
                selection, datetime.datetime.today().strftime("%Y-%m-%d")
            ),
            echo=False,
        )
        with engine.connect() as con:
            create_table_stmt = "select cache_data from cache"
            result = con.execute(create_table_stmt)
            for idx, row in enumerate(result):
                export_file.write(zlib.decompress(row[0]).decode("utf8") + "\n")

        print("start output {}:{}".format(selection, start_time))
        print(datetime.datetime.time(datetime.datetime.now()))


if __name__ == "__main__":
    main()
