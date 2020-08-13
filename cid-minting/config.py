import os

import lib.utils as utils

def get_config_by_key(config_dir_name, config_fname, key):
    """return config value by key from .yml config file
       config_dir: directory of configuration files
       config_fname: configuration filename
       key: configuration key
    """
    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, config_dir_name)

    # load all configuration files in directory
    configs = utils.load_config(CONFIG_PATH)

    # get config value by filename and key
    config = configs.get(config_fname, {}).get(key)
    return config
