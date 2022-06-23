import os
from os.path import join, dirname
import sys

import environs
import logging
import json

from lib.utils import db_connect_url
from lib.utils import get_configs_by_filename
from cid_minter.local_cid_minter import prepare_database
from cid_minter.local_cid_minter import find_cids_by_ocns
from cid_minter.local_cid_minter import find_cid_by_sysid
from cid_minter.local_cid_minter import insert_a_record
from cid_minter.local_cid_minter import invalid_sql_in_clause_str

def usage(script_name):
        print("Usage: {} env[dev|stg|prd] action[read|write] type[ocn|sysid] data[comma_separated_ocns|sys_id] cid".format(script_name))
        print("{} dev read ocn 8727632,32882115".format(script_name))
        print("{} dev read sysid uc1234567".format(script_name))
        print("{} dev write ocn 30461866 011323406".format(script_name))
        print("{} dev write sysid uc1234567 011323407".format(script_name))

def main():
    """ Performs read and write actions to the cid_minting_store table which stores the identifier and CID in pairs.
        Command line arguments:
        argv[1]: Server environemnt (Required). Can be test, dev, stg, or prd.
        argv[2]: Action. Can only be 'read' or 'write'
        argv[3]: Data type. Can only be 'ocn' and 'sysid'
        argv[4]: Data. OCNs or a local system ID.
                 OCNs format:
                   Comma separated strings without spaces in between any two values.
                   For example: 8727632,32882115
                 Local system ID: a string.
        argv[5]: A CID. Only required when Action='write'
    """

    if (len(sys.argv) != 5 and len(sys.argv) != 6):
        print("Parameter error.")
        usage(sys.argv[0])
        exit(1)

    env = sys.argv[1]
    action = sys.argv[2]
    data_type = sys.argv[3]
    data = sys.argv[4]
    cid = None
    if len(sys.argv) == 6:
        cid = sys.argv[5]

    if env not in ["test", "dev", "stg", "prd"]:
        usage(sys.argv[0])
        exit(1)

    if action not in ["read", "write"]:
        usage(sys.argv[0])
        exit(1)

    if data_type not in ["ocn", "sysid"]:
        usage(sys.argv[0])
        exit(1)

    if action == "write" and cid == None:
        usage(sys.argv[0])
        exit(1)

    cmd_options = "cmd options: {} {} {} {}".format(env, action, data_type, data)
    if cid:
        cmd_options += " " + cid

    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, 'config')

    configs= get_configs_by_filename(CONFIG_PATH, 'cid_minting')
    logfile = configs['logpath']
    db_config = str(db_connect_url(configs[env]['minter_db']))

    logging.basicConfig(
            level=logging.DEBUG,
            filename=logfile,
            format="%(asctime)s %(levelname)-4s %(message)s",
        )
    logging.info("Start " + os.path.basename(__file__))
    logging.info(cmd_options)

    DB_CONNECT_STR = os.environ.get('OVERRIDE_DB_CONNECT_STR') or db_config

    db = prepare_database(DB_CONNECT_STR)
    engine = db['engine']
    session = db['session']
    CidMintingStore = db['table']

    results = {} 
    if action == "read":
        if data_type == "ocn":
            results = find_cids_by_ocns(engine, data.split(","))

        if data_type == "sysid":
            results = find_cid_by_sysid(CidMintingStore, session, data)
        
        engine.dispose()
        print(json.dumps(results))
        exit(0)

    if action == "write":
        record = CidMintingStore(type=data_type, identifier=data, cid=cid)
        inserted = insert_a_record(session, record)
        engine.dispose()
        if inserted != "Success":
            exit(1)
        else:
            exit(0)

if __name__ == "__main__":
    main()
