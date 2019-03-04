#!/usr/bin/env python
"""Find_cluster.py: Find the cluster identifier (CID) for an item given
a contribsys_id (local system number) or an OCLC """

import argparse
import csv
import os
import sys

from environs import Env
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
import yaml

from lib.utils import ConsoleMessenger
import lib.utils as utils


def main(argv=None):
    # APPLICATION SETUP
    # load environment
    env = Env()
    env.read_env()

    ROOT_PATH = os.environ.get("ZEPHIR_ROOT_PATH") or os.path.dirname(__file__)
    ENV = os.environ.get("ZEPHIR_ENV")
    CONFIG_PATH = os.environ.get("ZEPHIR_CONFIG_PATH") or os.path.join(
        ROOT_PATH, "config"
    )
    OVERRIDE_CONFIG_PATH = os.environ.get("ZEPHIR_OVERRIDE_CONFIG_PATH")

    # load all configuration files in directory
    config = utils.load_config(CONFIG_PATH)

    # used in testing, config files in test data will override local config files
    if OVERRIDE_CONFIG_PATH is not None:
        config = utils.load_config(OVERRIDE_CONFIG_PATH, config)

    # load arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath", nargs="*", help="Filepath to files for processing")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Emit messages dianostic messages about everything.",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Don't emit non-error messages to stderr. Errors are still emitted silence those with 2>/dev/null.",
    )
    args = parser.parse_args()

    # Print handler to manage when/where messages should print
    console = ConsoleMessenger(args.quiet, args.verbose)

    # REQUIREMENTS
    if len(args.filepath) == 0:
        console.error("No files given to process.")
        sys.exit(1)

    # DATABASE SETUP
    # Create database client, connection manager.
    db = config.get("zephir-db", {}).get(ENV)

    DB_CONNECT_STR = str(utils.db_connect_url(db))

    engine = create_engine(DB_CONNECT_STR)

    # Create classes through reflection
    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Identifiers = Base.classes.zephir_identifiers

    # Create a session to the database.
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()

    # Iterate over the json log files to process
    for file in args.filepath:

        if not os.path.isfile(file):
            console.error("File path '{0}' does not exist. Exiting...".format(file))
            break

        # Open file and process
        with open(file) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            for row in csv_reader:
                console.report(row)

            # Example query
            # session = Session()
            # try:
            #     query = (
            #         session.query(Event.event_key)
            #         .filter(Event.timestamp >= query_params["first_timestamp"])
            #         .filter(Event.timestamp <= query_params["last_timestamp"])
            #         .filter(Event.type == query_params["event_type"])
            #     )
            #
            #     for event in query.all():
            #         db_events.add(event.event_key)
            # except Exception as e:
            #     session.rollback()
            #     raise
            # finally:
            #     session.close()

    console.report("Done!")
    sys.exit(0)


if __name__ == "__main__":
    main()
