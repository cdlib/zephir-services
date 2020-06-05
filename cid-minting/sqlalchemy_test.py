import os
import sys

import sqlalchemy as database 
import environs

from lib.utils import ConsoleMessenger
import lib.utils as utils

def main():
    #engine = db.create_engine('dialect+driver://user:pass@host:port/db')

    #db_conn_str = "sqlite:///database/test_sqlite.db"
    #db_conn_str = "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(user, passwd, host, port, db)

    if (len(sys.argv) > 1):
        env = sys.argv[1]
    else:
        env = "test"

    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    ENV = os.environ.get("ZED_ENV") or env
    print("env: {}".format(ENV))
    CONFIG_PATH = os.path.join(ROOT_PATH, "config")

    # load all configuration files in directory
    config = utils.load_config(CONFIG_PATH)
    print(config)

    # DATABASE SETUP
    # Create database client, connection manager.
    db = config.get('minter_db', {}).get(ENV)
    print(db)

    DB_CONNECT_STR = str(utils.db_connect_url(db))

    engine = database.create_engine(DB_CONNECT_STR)

    connection = engine.connect()
    metadata = database.MetaData()
    cid_minting_store = database.Table('cid_minting_store', metadata, autoload=True, autoload_with=engine)

    print(cid_minting_store.columns.keys())

    query = database.select([cid_minting_store])
    ResultProxy = connection.execute(query)
    ResultSet = ResultProxy.fetchall()

    print(ResultSet)



if __name__ == "__main__":
    main()
