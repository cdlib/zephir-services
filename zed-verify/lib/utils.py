import os

import environs
import sqlalchemy.engine.url
import yaml

def db_connect_url(config):

    drivername = os.environ.get('ZED_DB_DRIVERNAME') or config.get("drivername")
    username = os.environ.get('ZED_DB_USERNAME') or config.get("username")
    password = os.environ.get('ZED_DB_PASSWORD') or config.get("password")
    host = os.environ.get('ZED_DB_HOST') or config.get("host")
    port = os.environ.get('ZED_DB_PORT') or config.get("port")
    database = os.environ.get('ZED_DB_DATABASE') or config.get("database")
    socket = os.environ.get('ZED_DB_SOCKET') or config.get("socket")

    url = sqlalchemy.engine.url.URL(
        drivername,
        username,
        password,
        host,
        port,
        database,
    )

    if drivername == "mysql+mysqlconnector" and socket is not None:
        url = url + "?unix_socket=" + socket

    return url

def load_config(path, config={}):
    config = _load_config_dir(path, config)
    return config

def _load_config_dir(path, config):
    for entry in os.scandir(path):
        if entry.is_file() and entry.name.endswith(".yml"):
            section = os.path.splitext(entry.name)[0]
            with open(entry, "r") as ymlfile:
                config[section] = {}
                config[section].update(yaml.load(ymlfile))
    return config
