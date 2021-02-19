import datetime
import os
import zlib

import click
import mysql.connector
from sqlalchemy import create_engine

import lib.utils_refactor as utils

exec(compile(source=open('shared_cli.py').read(), filename='shared_cli.py', mode='exec'))


# CID REPO STATUS CLI SIGNATURE
@click.command("cid-repo-status")
@pass_state
@common_verbose_options
@click.option(
    "--status",
    type=click.Choice(["present", "absent", "all"], case_sensitive=False),
    default="all",
)
@click.option(
    "-o",
    "--output-filepath",
    help="Filepath to write status output",
)
@click.argument(
    "input-filepath",
    required=True,
    nargs=-1,
    callback=path_callback,
)
def cid_repo_status_cli(state, **kwargs):
    """ Retrieve the status of CIDs within the HathiTrust Digital Repository"""
    try:
        app = utils.application_setup(
            root_dir=os.path.join(os.path.dirname(__file__)), state=state, kwargs=kwargs
        )
        return_code = cid_repo_status_cmd(app)
    except Exception as err:
        raise err
        return_code = 1
        raise click.ClickException(err)
    click.Context.exit(return_code)

# CID REPO STATUS COMMAND
def cid_repo_status_cmd(app):
    """ Status for CIDs"""
    """

    Args:
        AppEnv (Complete CLI context for configuration and arguments)

    Returns: Exit code

        """
    STATUS_ALL = "all"
    STATUS_ABSENT = "absent"
    STATUS_PRESENT = "present"
    cids = []
    count = 0
    for input_filepath in app.state.input_filepaths:
        app.console.debug("Reading input file: {}".format(input_filepath))
        with open(input_filepath) as file:
            for line in file:
                stripped_line = line.strip()
                cids.append(stripped_line)
                count += 1
    app.console.debug("CIDs read: {}".format(count))
    cids = list(set(cids))
    cids.sort()
    cids_not_in_repo = []

    # DATABASE
    app.console.info(
        "Checking HathiTrust Repository status for {0} CIDs out of {1} given".format(
            app.args["status"].lower(), count
        )
    )
    # setup configuration
    db = app.CONFIG.get("database", {}).get(app.ENV)
    db_helper = utils.DatabaseHelper(config=db, env_prefix="ZEPHIR")
    conn_args = db_helper.connection_args()
    # select cids that have only holdings/items with a NULL ingest date using subquery
    SQL_STMT = "select distinct cid from zephir_records where attr_ingest_date is null and cid in ({0}) and cid not in (select distinct cid from zephir_records as is_not_null where attr_ingest_date is not null and cid in ({0}))".format(
        ",".join(cids)
    )

    try:
        # connect to datbase
        db_connection = mysql.connector.connect(**conn_args)
        app.console.debug(
            "Querying database w/: {}".format(
                utils.replace_key(conn_args, "password", "****")
            )
        )
        try:
            # execute query and iterate results
            db_cursor = db_connection.cursor()
            db_cursor.execute(SQL_STMT)

            for row in db_cursor:
                cids_not_in_repo.append(row[0])
        finally:
            db_cursor.close()

    finally:
        db_connection.close()

    # Find cids in repository through subjection
    cids_in_repo = list(set(cids) - set(cids_not_in_repo))

    #  Sort lists
    cids_in_repo.sort()
    cids_not_in_repo.sort()

    # Simplify variables
    status_type = app.args["status"].lower()
    output_filepath = app.args.get("output_filepath", None)
    if output_filepath:
        output_filepath = datetime.now().strftime(output_filepath)

    # Print console output
    if status_type == STATUS_PRESENT:
        # CID exists in HT Repository
        app.console.info("CIDs in the HathiTrust Repository...")
        app.console.out("\n".join(cids_in_repo))
    if status_type == STATUS_ABSENT:
        # CID is missing in HT Repository
        app.console.info("CIDs missing from the HathiTrust Repository...")
        app.console.out("\n".join(cids_not_in_repo))
    if status_type == STATUS_ALL:
        # Display both missing and existing CIDs
        app.console.info("CIDs status in the HathiTrust Repository...")
        for cid in cids:
            if cid in cids_in_repo:
                app.console.out(click.style("+" + cid, fg="green"))
            if cid in cids_not_in_repo:
                app.console.out(click.style("-" + cid, fg="red"))

    # Write to output file (if ouptut exists)
    if output_filepath:
        app.console.debug("Writing to output file: {}".format(output_filepath))
        with open(output_filepath, "w") as file:
            if status_type == STATUS_PRESENT:
                # CID exists in HT Repository
                file.write("\n".join(cids_in_repo)+"\n")
            if status_type == STATUS_ABSENT:
                # CID is missing in HT Repository
                file.write("\n".join(cids_not_in_repo)+"\n")
            if status_type == STATUS_ALL:
                # Display both missing and existing CIDs
                for cid in cids:
                    if cid in cids_in_repo:
                        file.write("+" + cid + "\n")
                    if cid in cids_not_in_repo:
                        file.write("-" + cid + "\n")

    return 0


if __name__ == "__main__":
    cid_repo_status_cli()
