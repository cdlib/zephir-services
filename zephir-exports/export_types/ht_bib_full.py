#!/usr/bin/env python

import datetime
import os
import socket
import zlib

from environs import Env
from sqlalchemy import create_engine

from export_cache import ExportCache
from vufind_formatter import VufindFormatter
from lib.new_utils import ConsoleMessenger
import lib.new_utils as utils


def ht_bib_full(
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
    EXPORT_PATH = os.environ.get("ZEPHIR_EXPORT_PATH") or os.path.join(
        ROOT_PATH, "export"
    )

    # load all configuration files in directory
    config = utils.load_config(CONFIG_PATH)

    # used in testing, config files in test data will override local config files
    if OVERRIDE_CONFIG_PATH is not None and os.path.isdir(OVERRIDE_CONFIG_PATH):
        config = utils.load_config(OVERRIDE_CONFIG_PATH, config)

    export_filename = "ht_bib_export_full_{}.json".format(
        datetime.datetime.today().strftime("%Y-%m-%d")
    )

    cache = "sqlite:///{}/cache-{}-{}.db".format(
        CACHE_PATH, merge_version, datetime.datetime.today().strftime("%Y-%m-%d")
    )

    start_time = datetime.datetime.now()

    console.debug(cache)

    with open(os.path.join(EXPORT_PATH, export_filename), "a") as export_file:

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

    return os.path.join(EXPORT_PATH, export_filename)


if __name__ == "__main__":
    create_bib_export()
