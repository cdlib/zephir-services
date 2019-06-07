#!/usr/bin/env python

import datetime
import os
import socket
import zlib

from environs import Env
from sqlalchemy import create_engine

from export_cache import ExportCache
from vufind_formatter import VufindFormatter
from lib.utils import ConsoleMessenger
import lib.utils as utils


def ht_bib_full(
    console=None,
    merge_version=None,
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

    export_filename = "ht_bib_export_full_{}.json".format(
        datetime.datetime.today().strftime("%Y-%m-%d")
    )

    cache = "sqlite:///{}/cache-{}-{}.db".format(
        APP.CACHE_PATH, merge_version, datetime.datetime.today().strftime("%Y-%m-%d")
    )

    start_time = datetime.datetime.now()

    console.debug(cache)

    with open(os.path.join(APP.EXPORT_PATH, export_filename), "a") as export_file:

        engine = create_engine(cache, echo=False)

        with engine.connect() as con:
            create_table_stmt = "select cache_data from cache"
            result = con.execute(create_table_stmt)
            for idx, row in enumerate(result):
                export_file.write(zlib.decompress(row[0]).decode("utf8") + "\n")

        console.debug(
            "Finished: {} (Elapsed: {})".format(
                merge_version, str(datetime.datetime.now() - start_time)
            )
        )

    return os.path.join(APP.EXPORT_PATH, export_filename)


if __name__ == "__main__":
    create_bib_export()
