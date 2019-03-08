#!/usr/bin/env python

import datetime
import os
import socket
import zlib

import argparse
from environs import Env
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
        "-s", "--selection", action="store", help="Selection algorithm used for export"
    )
    parser.add_argument(
        "-p", "--prefix", action="store_true", help="Use a prefix for export"
    )
    args = parser.parse_args()
    selection = args.selection
    if selection is None:
        raise "Must pass a selection algorithm to use. See --help"
    export_filename = "ht_bib_export_full_{}.json".format(
        datetime.datetime.today().strftime("%Y-%m-%d")
    )
    if args.prefix:
        export_filename = "{}-{}".format(selection, export_filename)

    start_time = datetime.datetime.now()

    with open(
        os.path.join(os.path.dirname(__file__), "export/{}".format(export_filename)),
        "a",
    ) as export_file:

        engine = create_engine(
            "sqlite:///{}/cache-{}-{}.db".format(
                os.path.join(os.path.dirname(__file__), "cache"),
                selection,
                datetime.datetime.today().strftime("%Y-%m-%d"),
            ),
            echo=False,
        )

        with engine.connect() as con:
            create_table_stmt = "select cache_data from cache"
            result = con.execute(create_table_stmt)
            for idx, row in enumerate(result):
                export_file.write(zlib.decompress(row[0]).decode("utf8") + "\n")

        print(
            "Finished: {} (Elapsed: {})".format(
                selection, str(datetime.datetime.now() - start_time)
            )
        )


if __name__ == "__main__":
    main()
