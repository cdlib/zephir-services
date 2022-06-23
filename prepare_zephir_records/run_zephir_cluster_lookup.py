import os
import sys
import re

from sqlalchemy import create_engine
from sqlalchemy import text

from lib.utils import db_connect_url
from lib.utils import get_configs_by_filename
from cid_minter.zephir_cluster_lookup import  ZephirDatabase:

def main():
    if (len(sys.argv) > 1):
        env = sys.argv[1]
    else:
        env = "test"

    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, 'config')

    configs= get_configs_by_filename(CONFIG_PATH, 'zephir_db')
    print(configs)

    db_conn_str = str(db_connect_url(configs[env]))
    zephirDb = ZephirDatabase(db_conn_str)

    ocns_list = [6758168, 15437990, 5663662, 33393343, 28477569, 8727632]
    print("Inquiry OCNs: {}".format(ocns_list))
    results = zephirDb.zephir_clusters_lookup(ocns_list)
    print(results)

    sysid_list = ['pur63733', 'nrlf.b100608668']
    results = zephirDb.zephir_clusters_lookup_by_sysids(sysid_list)
    print(results)

if __name__ == '__main__':
    main()
