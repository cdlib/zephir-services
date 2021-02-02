import datetime
import os
import zlib

import click
import mysql.connector
from sqlalchemy import create_engine

import lib.utils_refactor as utils


def cids_repo_status_cmd(app):
    """ Output the HT repository status for CIDs"""
    """

    Args:
        TDB

    Returns:
        TBD


        """
    cids = []
    count = 0
    input_filepath = app.kwargs["input_path"]
    app.console.debug("Reading input file: {}".format(input_filepath))
    with open(input_filepath) as file:
        for line in file:
            stripped_line = line.strip()
            cids.append(stripped_line)
            count += 1
    app.console.debug("CIDs read: {}".format(count))
    cids.sort()
    cids_in_repo = []
    cids_not_in_repo = []

    try:
        app.console.info("Checking HathiTrust Repository for {0} CIDs out of {1} given".format(app.kwargs["list_type"].lower(),count))

        # DATABASE: Prepare connection and statements
        # get database connection
        db = app.CONFIG.get("database", {}).get(app.ENV or "development")
        db_config = utils.DatabaseHelper(config=db, env_prefix="ZEPHIR")
        conn_args = db_config.connection_args()
        app.console.debug(
            "Querying database w/: {}".format(
                utils.replace_key(conn_args, "password", "****")
            )
        )
        db_connection = mysql.connector.connect(**conn_args)
        # Find all CIDs not in the HathiTrust repository
        # select distinct cids that have items with a NULL ingest dates AND have no items with an ingest database
        SQL_STMT = "select distinct cid from zephir_records where attr_ingest_date is null and cid in ({0}) and cid not in (select distinct cid from zephir_records as is_not_null where attr_ingest_date is not null and cid in ({0}))".format(
            ",".join(cids)
        )
        db_cursor = db_connection.cursor()
        db_cursor.execute(SQL_STMT)

        for row in db_cursor:
            cids_not_in_repo.append(row[0])
        db_cursor.close()

    finally:
        db_cursor.close()
        db_connection.close()

    # Find cids in repository through subjection
    cids_in_repo = list(set(cids) - set(cids_not_in_repo))
    #  Sort lists
    cids_in_repo.sort()
    cids_not_in_repo.sort()

    # Print console output
    if app.kwargs["list_type"].lower() == "exists":
        app.console.info("CIDs in the HathiTrust Repository...")
        app.console.out("\n".join(cids_in_repo))
    if app.kwargs["list_type"].lower() == "missing":
        app.console.info("CIDs missing from the HathiTrust Repository...")
        app.console.out("\n".join(cids_not_in_repo))
    if app.kwargs["list_type"].lower() == "all":
        app.console.info("CIDs status in the HathiTrust Repository...")
        for cid in cids:
            if cid in cids_in_repo:
                app.console.out(click.style("+" + cid, fg="green"))
            if cid in cids_not_in_repo:
                app.console.out(click.style("-" + cid, fg="red"))

    # Wite to output file (if exists)
    output_filepath = app.kwargs.get("output_path", None)
    if output_filepath:
        app.console.debug("Writing to output file: {}".format(output_filepath))
        with open(output_filepath, "w") as file:
            if app.kwargs["list_type"].lower() == "exists":
                file.write("\n".join(cids_in_repo))
            if app.kwargs["list_type"].lower() == "missing":
                file.write("\n".join(cids_not_in_repo))
            if app.kwargs["list_type"].lower() == "all":
                for cid in cids:
                    if cid in cids_in_repo:
                        file.write("+" + cid + "\n")
                    if cid in cids_not_in_repo:
                        file.write("-" + cid + "\n")

    return True


if __name__ == "__main__":
    cids_repo_status_cmd()
