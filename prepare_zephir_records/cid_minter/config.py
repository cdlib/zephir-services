import os

import lib.utils as utils

def get_configs_by_filename(config_dir_name, config_file):
    """return configs defined in the config_file as a dictionary
       config_dir: directory of configuration files
       config_file: configuration filename
    """
    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, config_dir_name)

    # load all configuration files in directory
    configs = utils.load_config(CONFIG_PATH)

    return configs.get(config_file)

