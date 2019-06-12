#!/usr/bin/env python
"""Utils.py: Utils are a collection of methods used across scripts"""

import os
import sys

import sqlalchemy.engine.url
import yaml


def db_connect_url(config):
    """Database connection URL creates a connection string through configuration
    values passed. The method allows for environmental variable overriding.

    Notes: These strings depend on the sqlalchemy package.

    Args:
        config:  A dictionary of database configuration values.

    Returns:
        A database connection string compatable with sqlalchemy.

        """
    drivername = os.environ.get("ZEPHIR_DB_DRIVERNAME") or config.get("drivername")
    username = os.environ.get("ZEPHIR_DB_USERNAME") or config.get("username")
    password = os.environ.get("ZEPHIR_DB_PASSWORD") or config.get("password")
    host = os.environ.get("ZEPHIR_DB_HOST") or config.get("host")
    port = os.environ.get("ZEPHIR_DB_PORT") or config.get("port")
    database = os.environ.get("ZEPHIR_DB_DATABASE") or config.get("database")
    socket = os.environ.get("ZEPHIR_DB_SOCKET") or config.get("socket")

    url = str(
        sqlalchemy.engine.url.URL(drivername, username, password, host, port, database)
    )

    # if using mysql, add the socket to the URL
    if drivername == "mysql+mysqlconnector" and socket is not None:
        url = url + "?unix_socket=" + socket

    return url


def load_config(path, config={}):
    """Load configuration files in the configuration directory
    into a unified configuration dictionary.

    Notes: Configuration files must be yaml files. The names
    of the files become the top-level keys in the dictionary.

    Args:
        path: Path to a configuration directory.
        config:  An existing dictionary of configuration values.

    Returns:
        A configuration dictionary populated with the contents of the
        configuration files.

        """
    for entry in os.scandir(path):
        if entry.is_file() and entry.name.endswith(".yml"):
            section = os.path.splitext(entry.name)[0]
            with open(entry, "r") as ymlfile:
                config[section] = {}
                config[section].update(yaml.safe_load(ymlfile))
    return config


class ConsoleMessenger:
    """ConsoleMessenger Class provides utility functions for outputing
    messages to the console, which can be configured for both
    quiet and verbose flags. This eliminates having to track these
    flags and use conditional logic to know when to print specific
    messages.

    Args:
        quiet: A flag to suppress all output except errors
        verbose: A flag to print diagnostic messages
        """

    def __init__(self, quiet=False, verbose=False, very_verbose=False):
        self.quiet = quiet
        self.verbose = verbose or very_verbose
        self.very_verbose = very_verbose

    # verbose diagnostic messages only
    def info(self, message):
        if self.verbose:
            print(message, file=sys.stderr)

    # very verbose debug messages only
    def debug(self, message):
        if self.very_verbose:
            print(message, file=sys.stderr)

    # concise error handling messages
    def error(self, message):
        print(message, file=sys.stderr)

    # standard output for use by chained applications
    def out(self, message):
        if not self.quiet:
            print(message, file=sys.stdout)
