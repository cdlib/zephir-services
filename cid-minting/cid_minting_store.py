import os
import sys

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

import environs

from lib.utils import ConsoleMessenger
import lib.utils as utils

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

    # get config value by filename and key 
    config = configs.get(config_fname, {}).get(key)

    return str(utils.db_connect_url(config))

def prepare_database(db_connect_str):
    engine = create_engine(db_connect_str)
    session = Session(engine)

    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)
    # map table to class
    CidMintingStore = Base.classes.cid_minting_store
    return {
        "engine": engine, 
        "session": session, 
        "table": CidMintingStore}

def find_all(CidMintingStore, session):
    query = session.query(CidMintingStore)
    return query.all()

def find_by_identifier(CidMintingStore, session, data_type, value):
    query = session.query(CidMintingStore).filter(CidMintingStore.type==data_type).filter(CidMintingStore.identifier==value)

    record = query.first()
    return record

def find_query(engine, sql):
    with engine.connect() as connection:
        results = connection.execute(sql)
        return results.fetchall()

def insert_a_record(log, session, record):

    try:
        session.add(record)
    except Exception as e:
        session.rollback()
        raise
    else:
        session.commit()
    return

def main():
    #engine = db.create_engine('dialect+driver://user:pass@host:port/db')
    #db_conn_str = "sqlite:///database/test_sqlite.db"
    #db_conn_str = "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(user, passwd, host, port, db)

    if (len(sys.argv) > 1):
        env = sys.argv[1]
    else:
        env = "test"

    # get environment variable in .env file

    ENV = os.environ.get("MINTER_ENV") or env
    print("env: {}".format(ENV))
    
    DB_CONNECT_STR = os.environ.get("OVERRIDE_DB_CONNECT_STR") or get_db_conn_string_from_config_by_key('config', 'minter_db', ENV)
    print("db_connect_str {}".format(DB_CONNECT_STR))

    db = prepare_database(DB_CONNECT_STR)
    engine = db["engine"]
    session = db["session"]
    CidMintingStore = db["table"]

    sql = "select * from cid_minting_store"
    print(sql)
    results = find_query(engine, sql)
    print("find by sql: {}".format(sql))
    for record in results:
        print("type: {}, value: {}, cid {} ".format(record.type, record.identifier, record.cid))

    print("find all")
    results = find_all(CidMintingStore, session)
    for record in results:
        print("type: {}, value: {}, cid {} ".format(record.type, record.identifier, record.cid))

    print("find one by ocn")
    record = find_by_identifier(CidMintingStore, session, 'oclc', '8727632')
    if record:
        print("type: {}, value: {}, cid {} ".format(record.type, record.identifier, record.cid))

    print("find one by sys_id")
    record = find_by_identifier(CidMintingStore, session, 'contrib_sys_id', 'pur215476')
    if record:
        print("type: {}, value: {}, cid {} ".format(record.type, record.identifier, record.cid))

    print("add oclc=30461866")
    record = CidMintingStore(type='oclc', identifier='30461866', cid='011323406')
    #insert_a_record('log', session, record)
    record = find_by_identifier(CidMintingStore, session, 'oclc', '30461866')
    if record:
        print("type: {}, value: {}, cid {} ".format(record.type, record.identifier, record.cid))

if __name__ == "__main__":
    main()
