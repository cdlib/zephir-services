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
    drivername = config.get("drivername")
    username = config.get("username")
    password = config.get("password")
    host = config.get("host")
    port = config.get("port")
    database = config.get("database")

    url = str(
        sqlalchemy.engine.url.URL(drivername, username, password, host, port, database)
    )

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

def get_configs_by_filename(config_path, config_filename):
    """return configs defined in the config_file as a dictionary
       config_dir: directory of configuration files
       config_file: configuration filename
    """
    # load all configuration files in directory
    configs = load_config(config_path)

    return configs.get(config_filename)
