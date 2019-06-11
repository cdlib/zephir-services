#!/usr/bin/env python
"""Utils.py: Utils are a collection of methods used across scripts"""

import datetime
import os
import sys

import click
import environs
import sqlalchemy.engine.url
import yaml


class AppEnv:
    def __init__(self, name, root_dir=os.path.dirname(__file__)):
        self.name = name

        # load enviroment variables from .env file
        app_env = environs.Env()
        app_env.read_env()

        with app_env.prefixed("{}_".format(name)):
            self.ROOT_PATH = app_env("ROOT_PATH", False) or root_dir
            self.ENV = app_env("ENV", False)
            self.CONFIG_PATH = app_env("CONFIG_PATH", False) or os.path.join(
                self.ROOT_PATH, "config"
            )
            self.OVERRIDE_CONFIG_PATH = app_env("OVERRIDE_CONFIG_PATH", False)
            self.CACHE_PATH = app_env("CACHE_PATH", False) or os.path.join(
                self.ROOT_PATH, "cache"
            )
            self.IMPORT_PATH = app_env("IMPORT_PATH", False) or os.path.join(
                self.ROOT_PATH)
            self.OUTPUT_PATH = app_env("OUTPUT_PATH", False) or os.path.join(
                self.ROOT_PATH, "export"
            )
            # TODO(ccollett): Refactor this to output path
            self.EXPORT_PATH = app_env("EXPORT_PATH", False) or os.path.join(
                self.ROOT_PATH, "export"
            )
        # Load application config
        config = AppEnv._load_config(self.CONFIG_PATH)
        # used in testing, config files in test data will override local config files
        if self.OVERRIDE_CONFIG_PATH is not None and os.path.isdir(
            self.OVERRIDE_CONFIG_PATH
        ):
            config = AppEnv._load_config(self.OVERRIDE_CONFIG_PATH, config)
        self.CONFIG = config

    @staticmethod
    def _load_config(path, config={}):
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


class DatabaseHelper:
    """Database Helper stores and manages database configurations to
    a relational database. The class provides methods for instantiating database
    connections in different packages (sqlalchemy, mysql.connector)

    Notes: These strings depend on the sqlalchemy package.

    Args:
        config:  A dictionary of database configuration values.
        env_prefix: A prefix used to identify environment variables for script

    Returns:
        A database connection string compatable with sqlalchemy.

            """

    def __init__(self, config, env_prefix):
        self.drivername = os.environ.get(
            "{}_DB_DRIVERNAME".format(env_prefix)
        ) or config.get("drivername")
        self.username = os.environ.get(
            "{}_DB_USERNAME".format(env_prefix)
        ) or config.get("username")
        self.password = os.environ.get(
            "{}_DB_PASSWORD".format(env_prefix)
        ) or config.get("password")
        self.host = os.environ.get("{}_DB_HOST".format(env_prefix)) or config.get(
            "host"
        )
        self.port = os.environ.get("{}_DB_PORT".format(env_prefix)) or config.get(
            "port"
        )
        self.database = os.environ.get(
            "{}_DB_DATABASE".format(env_prefix)
        ) or config.get("database")
        self.socket = os.environ.get("{}_DB_SOCKET".format(env_prefix)) or config.get(
            "socket"
        )

    def connection_url(self):
        """
        Returns:
            A database connection string compatable with sqlalchemy.

        """

        url = str(
            sqlalchemy.engine.url.URL(
                self.drivername,
                self.username,
                self.password,
                self.host,
                self.port,
                self.database,
            )
        )

        # if using mysql, add the socket to the URL
        if drivername == "mysql+mysqlconnector" and self.socket is not None:
            url = "{}?unix_socket={}".format(url, self.socket)

        return url

    def connection_args(self):
        """
        Returns:
            A database arguments compatable with mysqlconnector.

        """

        args = {
            "user": self.username,
            "password": self.password,
            "host": self.host,
            "database": self.database,
            "unix_socket": self.socket,
        }

        return args


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

    def __init__(self, app=None, quiet=False, verbose=False, very_verbose=False):
        self.app = app
        self.quiet = quiet
        self.verbose = verbose or very_verbose
        self.very_verbose = very_verbose

    # verbose diagnostic messages only
    def info(self, message):
        if self.verbose:
            self.send_error(message, level="INFO")

    # very verbose debug messages only
    def debug(self, message):
        if self.very_verbose:
            self.send_error(message, level="DEBUG")

    # concise error handling messages
    def error(self, message):
            self.send_error(message, level="ERROR")

    # standard output for use by chained applications
    def out(self, message):
        if not self.quiet:
            click.secho(message, file=sys.stdout)

    def send_error(self, message, level=None):
        line = ""
        if self.very_verbose:
            line += datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f ")
        if level and self.very_verbose:
            line += level + " "
        if self.app and self.verbose:
            line += self.app + ": "
        line += message
        click.secho(line, file=sys.stderr)
