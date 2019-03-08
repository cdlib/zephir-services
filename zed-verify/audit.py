import datetime
import iso8601
import json
import os
import sys

import click
import environs
import sqlalchemy as sqla

from lib.utils import ConsoleMessenger
import lib.utils as utils


@click.command()
@click.argument("filepath", nargs=-1, type=click.Path(exists=True))
@click.option(
    "-q",
    "--quiet",
    is_flag=True,
    default=False,
    help="Filepath to files for processing",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=True,
    help="Emit messages dianostic messages about everything.",
)
@click.option(
    "-d",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Don't emit non-error messages to stderr. Errors are still emitted \
    silence those with 2>/dev/null.",
)
@click.option(
    "-s", "--suffix", "suffix", default="audited", help="for renaming valid files"
)
def audit(filepath, quiet, verbose, dry_run, suffix):
    """Audit.py: Audit ZED log file to ensure all the data is represented in
    the database"""
    # Print handler to manage when and how messages should print
    console = ConsoleMessenger(quiet, verbose)

    # REQUIREMENTS
    if len(filepath) == 0:
        console.error("No files given to process.")
        sys.exit(1)

    # APPLICATION SETUP
    # load environment
    env = environs.Env()
    env.read_env()

    ROOT_PATH = os.environ.get("ZED_ROOT_PATH") or os.path.dirname(__file__)
    ENV = os.environ.get("ZED_ENV")
    CONFIG_PATH = os.environ.get("ZED_CONFIG_PATH") or os.path.join(ROOT_PATH, "config")
    OVERRIDE_CONFIG_PATH = os.environ.get("ZED_OVERRIDE_CONFIG_PATH")

    # load all configuration files in directory
    config = utils.load_config(CONFIG_PATH)

    # used in testing, config files in test data will override local config files
    if OVERRIDE_CONFIG_PATH is not None:
        config = utils.load_config(OVERRIDE_CONFIG_PATH, config)

    # Print handler to manage when/where messages should print
    console = ConsoleMessenger(quiet, verbose)

    # DATABASE SETUP
    # Create database client, connection manager.
    db = config.get("zed_db", {}).get(ENV)

    DB_CONNECT_STR = str(utils.db_connect_url(db))

    engine = sqla.create_engine(DB_CONNECT_STR)

    # Create classes through reflection
    Base = sqla.ext.automap.automap_base()
    Base.prepare(engine, reflect=True)
    Event = Base.classes.events

    # Create a session to the database.
    Session = sqla.orm.sessionmaker()
    Session.configure(bind=engine)
    session = Session()

    if dry_run:
        console.diagnostic("DRY RUN")

    # Iterate over the json log files to process
    for file in filepath:

        if not os.path.isfile(file):
            console.error("File path '{0}' does not exist. Exiting...".format(file))
            break

        # # Get the file name, path, and create destination file name, path
        f_path, f_name = os.path.split(file)
        renamed_file = os.path.join("{0}.{1}".format(file, suffix))

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
                raise e
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
            if not dry_run:
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
    audit()
