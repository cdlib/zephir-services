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
    """AppEnv Class provides an easy helper for loading enviroment variables with
    default values and yaml configuration files into an object for Zephir Services.
    Enviroment variables include: ROOT_PATH, CONFIG_PATH, CACHE_PATH, IMPORT_PATH,
    and OUTPUT_PATH. OVERRIDE_CONFIG_PATH is a special environment variable that
    load after the CONFIG_PATH is loaded.

    Args:
        name: Prefix for application specific environment variables
        root: The default root directory.
    """

    def __init__(self, name, root_dir=os.path.dirname(__file__)):
        self.name = name
        self.console = None

        # load enviroment variables from .env file
        app_env = environs.Env()
        app_env.read_env()

        with app_env.prefixed("{}_".format(name)):
            self.ROOT_PATH = app_env("ROOT_PATH", False) or root_dir
            self.ENV = app_env("ENV", False)
            self.CONFIG_PATH = app_env("CONFIG_PATH", False) or os.path.join(
                self.ROOT_PATH, "config/"
            )
            self.OVERRIDE_CONFIG_PATH = app_env("OVERRIDE_CONFIG_PATH", False)

            self.CACHE_PATH = app_env("CACHE_PATH", False) or os.path.join(
                self.ROOT_PATH, "cache/"
            )
            self.INPUT_DIR = app_env("INPUT_DIR", False) or os.path.join(self.ROOT_PATH)
            self.OUTPUT_PATH = app_env("OUTPUT_DIR", False) or os.path.join(
                self.ROOT_PATH
            )
        # Load application config
        config = AppEnv._load_config(self.CONFIG_PATH)

        # Used in testing, config files in test data will override local config files
        if self.OVERRIDE_CONFIG_PATH is not None and os.path.isdir(
            self.OVERRIDE_CONFIG_PATH
        ):
            config = AppEnv._load_config(self.OVERRIDE_CONFIG_PATH, config)
        # return configuration (loaded or overide)
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
        if self.drivername == "mysql+mysqlconnector" and self.socket is not None:
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


def application_setup(root_dir=None, state=None, kwargs={}):
    # LOAD: environment, configuration
    default_root_dir = os.path.join(os.path.dirname(__file__))
    app = AppEnv(
        name="ZEPHIR",
        root_dir=root_dir,
    )
    app.state = state
    app.args = kwargs

    app.console = ConsoleMessenger(app="ZEPHIR-EXPORT", verbosity=int(state.verbosity))
    app.console.debug("Loading application...")
    app.console.debug("Environment: {}".format(app.ENV))
    app.console.debug("Configuration: {}".format(app.CONFIG_PATH))

    return app


class ConsoleMessenger:
    """ConsoleMessenger Class provides utility functions for outputing
    messages to the console, which can be configured for verbosity.
    This eliminates having to track these conditional logic to know when to print
    specific messages.

    Args:
        app: The name of the application (to prepend stderr messages)
        verbosity: verbosity level of application
            * -2: silent [No stdout, No stderr]
            * -1: quiet [No stdout, ERROR stderr]
            * 0: default [stdout, ERROR stderr]
            * 1: verbose [stdout, INFO stderr]
            * 2: very_verbose [stdout, DEBUG stderr]

    """

    def __init__(self, app=None, verbosity=0):
        self.app = app
        self.verbosity = verbosity

    # verbose diagnostic messages only
    def info(self, message):
        if self.verbose():
            self.send_error(message, level="INFO")

    # very verbose debug messages only
    def debug(self, message):
        if self.very_verbose():
            self.send_error(message, level="DEBUG")

    # concise error handling messages
    def error(self, message):
        self.send_error(message, level="ERROR")

    # standard output for use by chained applications
    def out(self, message):
        if not self.silent():
            click.secho(message, file=sys.stdout)

    def send_error(self, message, level=None):
        line = ""
        if self.very_verbose():
            line += datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f ")
        if level and self.very_verbose():
            line += level + " "
        if self.app:
            line += self.app + ": "
        line += message
        click.secho(line, file=sys.stderr)

    def silent(self):
        return self.verbosity == -2

    def quiet(self):
        return self.verbosity == -1

    def default(self):
        return self.verbosity == 0

    def verbose(self):
        return self.verbosity >= 1

    def very_verbose(self):
        return self.verbosity >= 2


def replace_key(dictionary, key, value):
    new_dict = dict(dictionary)
    if key in new_dict:
        new_dict[key] = value
    return new_dict
