import os
import sys

import sqlalchemy as sqla 
import sqlalchemy.ext.automap as sqla_automap 
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
    print(configs)

    # get config value by filename and key 
    config = configs.get(config_fname, {}).get(key)
    print(config)

    return str(utils.db_connect_url(config))

def define_session(db_connect_str):
    engine = sqla.create_engine(db_connect_str)

    # Create classes through reflection
    Base = sqla_automap.automap_base()
    Base.prepare(engine, reflect=True)
    CidMintingStore = Base.classes.cid_minting_store

    # Create a session to the database.
    Session = sqla.orm.sessionmaker()
    Session.configure(bind=engine)
    session = Session()
    return (CidMintingStore, session)

def find_all(CidMintingStore, session):
    query = session.query(CidMintingStore)

    for rd in query.all():
        print("type: {}, value: {}, cid {} ".format(rd.type, rd.identifier, rd.cid))
    return query.all()

def find_by_ocn(CidMintingStore, session, ocn):
    query = session.query(CidMintingStore).filter(CidMintingStore.type=='oclc').filter(CidMintingStore.identifier==ocn)

    record = query.first()
    if record:
        print("type: {}, value: {}, cid {} ".format(record.type, record.identifier, record.cid))
    return record

def find_query(db_conn_str, sql):
    engine = sqla.create_engine(db_conn_str)

    with engine.connect() as connection:
        results = connection.execute(sql)
        return results.fetchall()

def insert_a_record():
    pass

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

    sql = "select * from cid_minting_store"
    result = find_query(DB_CONNECT_STR, sql)

    print("find by sql: {}".format(sql))
    print(result)

    # mapping class, define session
    CidMintingStore, session = define_session(DB_CONNECT_STR)
    print("find all")
    find_all(CidMintingStore, session)
    print("find one")
    find_by_ocn(CidMintingStore, session, '8727632')

if __name__ == "__main__":
    main()
