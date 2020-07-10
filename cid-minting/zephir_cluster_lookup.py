import os
import sys
import environs

from sqlalchemy import create_engine
from sqlalchemy import text

from lib.utils import ConsoleMessenger
import lib.utils as utils

SELECT_ZEPHIR = """select distinct z.cid, i.identifier
    from zephir_records as z
    inner join zephir_identifier_records as r on r.record_autoid = z.autoid
    inner join zephir_identifiers as i on i.autoid = r.identifier_autoid
    where i.type = 'oclc'
"""
AND_IDENTIFIER_IN = "and i.identifier in"
ORDER_BY = "order by z.cid, z.id"

def get_db_conn_string_from_config_by_key(config_dir_name, config_fname, key):
    """return database connection string from db_config.yml file
       config_dir: directory of configuration files
       config_fname: configuration filename
       key: configuration key
    """
    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, config_dir_name)

    # load all configuration files in directory
    configs = utils.load_config(CONFIG_PATH)
    print(configs)

    # get config value by filename and key
    config = configs.get(config_fname, {}).get(key)
    print(config)

    return str(utils.db_connect_url(config))

class ZephirDatabase:
    def __init__(self, db_connect_str):
        self.engine = create_engine(db_connect_str)

    def query(self, sql, params=None):
        with self.engine.connect() as connection:
            results = connection.execute(sql, params or ())
            return results.fetchall()

def get_zephir_cluster_by_ocn(db_conn_str, oclc_list):
    sql = SELECT_ZEPHIR + " " + AND_IDENTIFIER_IN + " (" + oclc_list + ") " + ORDER_BY
    z = ZephirDatabase(db_conn_str)
    return z.query(text(sql))

def valid_ocn_list(ocn_list):
    pass

def main():
    if (len(sys.argv) > 1):
        env = sys.argv[1]
    else:
        env = "test"

    # get environment variable in .env file
    ENV = os.environ.get("MINTER_ENV") or env
    print("env: {}".format(ENV))

    DB_CONNECT_STR = os.environ.get("OVERRIDE_DB_CONNECT_STR") or get_db_conn_string_from_config_by_key('config','minter_db', ENV)

    print(DB_CONNECT_STR)

    oclc_list = "'6758168','15437990','5663662','33393343','28477569','8727632'"

    results = get_zephir_cluster_by_ocn(DB_CONNECT_STR, oclc_list)
    print(results)


if __name__ == '__main__':
    main()
