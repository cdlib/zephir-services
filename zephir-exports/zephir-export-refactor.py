from datetime import datetime
import os
import glob
import sys

import click

from cid_repo_status_cli import cid_repo_status_cli
import lib.utils_refactor as utils


@click.group()
@click.pass_context
def entry_point(ctx):
    """Zephir-exports is a command-line program to export data from Zephir in various
    formats for use by HathiTrust and contributors. It is designed to be used mainly by cron for daily exports."""


# ZEPHIR-EXPORT COMMANDS
entry_point.add_command(cid_repo_status_cli, "cid-repo-status")


if __name__ == "__main__":
    entry_point()
