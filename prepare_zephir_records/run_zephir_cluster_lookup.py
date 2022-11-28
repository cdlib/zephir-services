import os
import sys
import re

import argparse
from sqlalchemy import create_engine
from sqlalchemy import text

from lib.utils import db_connect_url
from lib.utils import get_configs_by_filename
from cid_minter.zephir_cluster_lookup import ZephirDatabase
from cid_minter.zephir_cluster_lookup import CidMinterTable

def main():
    parser = argparse.ArgumentParser(description='Lookup Zephir clusters by ONCs or SysIDs.')
    parser.add_argument('--env', '-e', nargs='?', dest='env', choices=["test", "dev", "stg", "prd"], required=True)
    parser.add_argument('--type', '-t', nargs='?', dest='type', choices=["ocn", "sysid"], required=True, help="ID type. Can be ocn or sysid")
    parser.add_argument('ids', help="OCNs or sysIDs separated by a comma without spaces. For example: 6758168,15437990 or pur63733,nrlf.b100608668")

    args = parser.parse_args()
    env = args.env
    id_type = args.type
    ids = args.ids.split(",")

    print(args)

    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, 'config')

    configs= get_configs_by_filename(CONFIG_PATH, 'zephir_db')
    print(configs)

    db_conn_str = str(db_connect_url(configs[env]))
    zephirDb = ZephirDatabase(db_conn_str)

    if id_type == "ocn":
        #ocns_list = [6758168, 15437990, 5663662, 33393343, 28477569, 8727632]
        print("Inquiry OCNs: {}".format(ids))
        results = zephirDb.zephir_clusters_lookup(ids)
        print(results)
    elif id_type == "sysid":
        #sysid_list = ['pur63733', 'nrlf.b100608668']
        print("Inquiry sysids: {}".format(ids))
        results = zephirDb.zephir_clusters_lookup_by_sysids(ids)
        print(results)


if __name__ == '__main__':
    main()
