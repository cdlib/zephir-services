import os
import sys

import sqlalchemy as sqla 
import sqlalchemy.ext.automap as sqla_automap 
import environs

from lib.utils import ConsoleMessenger
import lib.utils as utils

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

def find_one(CidMintingStore, session, ocn):
    query = session.query(CidMintingStore).filter(CidMintingStore.type=='oclc').filter(CidMintingStore.identifier==ocn)

    for rd in query.all():
        print("type: {}, value: {}, cid {} ".format(rd.type, rd.identifier, rd.cid))

    
def main():
    #engine = db.create_engine('dialect+driver://user:pass@host:port/db')

    #db_conn_str = "sqlite:///database/test_sqlite.db"
    #db_conn_str = "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(user, passwd, host, port, db)

    if (len(sys.argv) > 1):
        env = sys.argv[1]
    else:
        env = "test"

    # load environment in .env file
    env = environs.Env()
    env.read_env()

    ENV = os.environ.get("MINTER_ENV") or env
    print("env: {}".format(ENV))

    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, "config")

    # load all configuration files in directory
    config = utils.load_config(CONFIG_PATH)
    print(config)

    # DATABASE SETUP 
    # config/minter_db.xml
    # Create database client, connection manager.
    db = config.get('minter_db', {}).get(ENV)
    print(db)

    DB_CONNECT_STR = str(utils.db_connect_url(db))
    engine = sqla.create_engine(DB_CONNECT_STR)

    connection = engine.connect()
    metadata = sqla.MetaData()
    cid_minting_store = sqla.Table('cid_minting_store', metadata, autoload=True, autoload_with=engine)

    print(cid_minting_store.columns.keys())

    query = sqla.select([cid_minting_store])
    ResultProxy = connection.execute(query)
    ResultSet = ResultProxy.fetchall()

    print(ResultSet)


    CidMintingStore, session = define_session(DB_CONNECT_STR)
    find_all(CidMintingStore, session)
    find_one(CidMintingStore, session, '1')

if __name__ == "__main__":
    main()
