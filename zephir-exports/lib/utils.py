import os

import yaml


def zephir_config(environment, path):
    config = {}
    config["env"] = environment
    for entry in os.scandir(path):
        if entry.is_file() and entry.name.endswith(".yml"):
            section = os.path.splitext(entry.name)[0]
            with open(entry, "r") as ymlfile:
                config[section] = {}
                config[section].update(yaml.load(ymlfile))
    return config
