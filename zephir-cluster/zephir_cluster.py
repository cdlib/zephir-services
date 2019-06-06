import os

from environs import Env
import mysql.connector

import lib.utils as utils

class ZephirCluster:

    def __init__(self):
        pass

    @staticmethod
    def find_candidate_clusters(oclcs):

        # APPLICATION SETUP
        # load environment
        env = Env()
        env.read_env()

        ROOT_PATH = os.environ.get("ZEPHIR_ROOT_PATH") or os.path.join(
            os.path.dirname(__file__)
        )
        ENV = os.environ.get("ZEPHIR_ENV")
        CONFIG_PATH = os.environ.get("ZEPHIR_CONFIG_PATH") or os.path.join(
            ROOT_PATH, "config"
        )
        OVERRIDE_CONFIG_PATH = os.environ.get("ZEPHIR_OVERRIDE_CONFIG_PATH")
        CACHE_PATH = os.environ.get("ZEPHIR_CACHE_PATH") or os.path.join(ROOT_PATH, "cache")

        # load all configuration files in directory
        config = utils.load_config(CONFIG_PATH)

        # used in testing, config files in test data will override local config files
        if OVERRIDE_CONFIG_PATH is not None:
            config = utils.load_config(OVERRIDE_CONFIG_PATH, config)

        db = config.get("database", {}).get(ENV)

        sql_select = (
        f"select zr.cid from zephir_identifier_records zir "
        f"join zephir_identifiers zi on zir.identifier_autoid = zi.autoid "
        f"join zephir_records zr on zr.autoid = zir.record_autoid "
        f"where (type='oclc' and identifier in ('1570562')) "
        f"or (type = 'contrib_sys_id' and identifier= '') "
        f"order by zi.type desc, cid; ")

        candidate_list = []
        try:
            conn_args = {
                "user": db.get("username", None),
                "password": db.get("password", None),
                "host": db.get("host", None),
                "database": db.get("database", None),
                "unix_socket": None,
            }

            socket = os.environ.get("ZEPHIR_DB_SOCKET") or config.get("socket")

            if socket:
                conn_args["unix_socket"] = socket

            conn = mysql.connector.connect(**conn_args)

            cursor = conn.cursor()
            cursor.execute(sql_select)

            for idx, cid_row_result in enumerate(cursor):
                candidate_list.append(cid_row_result[0])

        finally:
            cursor.close()
            conn.close()

        return candidate_list
