#!/usr/bin/env python
"""Audit.py: Audit ZED log file to ensure all the data is represented in
the database

author: Charlie Collett"
copyright: Copyright 2018 The Regents of the University of California. All
rights reserved."""

import argparse
import datetime
import iso8601
import json
import os
import socket
import sys

from environs import Env
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
import yaml

from lib.console_messenger import ConsoleMessenger
import lib.utils as utils

def main(argv=None):
    # APPLICATION SETUP
    # load environment
    env = Env()
    env.read_env()

    ROOT_PATH = os.environ.get("ZED_ROOT_PATH") or os.path.dirname(__file__)
    ENV =  os.environ.get("ZED_ENV")
    CONFIG_PATH = os.environ.get("ZED_CONFIG_PATH") or os.path.join(ROOT_PATH, "config")
    OVERRIDE_CONFIG_PATH = os.environ.get("ZED_OVERRIDE_CONFIG_PATH")

    # load all configuration files in directory
    config = utils.load_config(CONFIG_PATH)

    # used in testing, config files in test data will override local config files
    if OVERRIDE_CONFIG_PATH is not None:
        config = utils.load_config(OVERRIDE_CONFIG_PATH, config)

    # load arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath", nargs="*", help="Filepath to files for processing")
    parser.add_argument(
        "-d", "--dry-run", action="store_true", help="Run process as rehearsal only"
    )
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
    parser.add_argument(
        "-s",
        "--suffix",
        action="store",
        default="audited",
        help="for renaming passing files",
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
    db = config.get("zed_db",{}).get(ENV)

    DB_CONNECT_STR = str(
        utils.db_connect_url(db)
    )

    engine = create_engine(DB_CONNECT_STR)

    # TODO(cscollett): print connection string w/out password to diagnostic.

    # Create classes through reflection
    Base = automap_base()
    Base.prepare(engine, reflect=True)
    Event = Base.classes.events

    # Create a session to the database.
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()

    if args.dry_run:
        console.diagnostic("DRY RUN")

    # Iterate over the json log files to process
    for file in args.filepath:

        if not os.path.isfile(file):
            console.error("File path '{0}' does not exist. Exiting...".format(file))
            break

        # # Get the file name, path, and create destination file name, path
        f_path, f_name = os.path.split(file)
        renamed_file = os.path.join("{0}.{1}".format(file, args.suffix))

        if os.path.isfile(renamed_file):
            console.error("Audit file '{0}' already exists.".format(renamed_file))
            break

        log_events = []
        db_events = set()
        file_pass = True  # Assume valid until line found invalid
        # Open file and process
        with open(file) as f_io:
            ln_cnt = 0
            console.diagnostic("Auditing: " + file)
            for line in f_io:
                ln_cnt += 1
                try:
                    log_events.append(json.loads(line.strip()))
                except json.decoder.JSONDecodeError:
                    file_pass = False
                    console.error("ERROR: Innvalid JSON on line {0}".format(ln_cnt))
                    break  # invalid json, stop successive validation routines

        if file_pass:
            query_params = {
                "event_type": log_events[0]["type"],
                "first_timestamp": (
                    iso8601.parse_date(log_events[0]["timestamp"])
                    - datetime.timedelta(seconds=60)
                ).isoformat("T"),
                "last_timestamp": (
                    iso8601.parse_date(log_events[-1]["timestamp"])
                    + datetime.timedelta(seconds=60)
                ).isoformat("T"),
            }

            session = Session()
            try:
                query = (
                    session.query(Event.event_key)
                    .filter(Event.timestamp >= query_params["first_timestamp"])
                    .filter(Event.timestamp <= query_params["last_timestamp"])
                    .filter(Event.type == query_params["event_type"])
                )

                for event in query.all():
                    db_events.add(event.event_key)
            except Exception as e:
                session.rollback()
                raise
            finally:
                session.close()

            for event in log_events:
                if not event["event"] in db_events:
                    file_pass = False
                    console.error(
                        "ERROR: Missing event {0} in database.".format(event["event"])
                    )

        # Report results
        if file_pass is False:
            console.error("File {0}: fail.".format(file))
        else:
            if not args.dry_run:
                os.rename(file, renamed_file)
            console.report(
                "File {0}: pass. {1} event(s) audited.\
            ".format(
                    file, len(log_events)
                )
            )

    console.report("Done!")
    sys.exit(0)


if __name__ == "__main__":
    main()
