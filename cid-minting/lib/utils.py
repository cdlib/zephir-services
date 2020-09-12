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
    drivername = os.environ.get("ZED_DB_DRIVERNAME") or config.get("drivername")
    username = os.environ.get("ZED_DB_USERNAME") or config.get("username")
    password = os.environ.get("ZED_DB_PASSWORD") or config.get("password")
    host = os.environ.get("ZED_DB_HOST") or config.get("host")
    port = os.environ.get("ZED_DB_PORT") or config.get("port")
    database = os.environ.get("ZED_DB_DATABASE") or config.get("database")
    socket = os.environ.get("ZED_DB_SOCKET") or config.get("socket")

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


