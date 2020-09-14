import os
import sys

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import IntegrityError

import environs
import logging

import lib.utils as utils
from config import get_configs_by_filename
from zephir_cluster_lookup import list_to_str
from zephir_cluster_lookup import valid_sql_in_clause_str
from zephir_cluster_lookup import invalid_sql_in_clause_str


def prepare_database(db_connect_str):
    engine = create_engine(db_connect_str)
    session = Session(engine)

    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)
    # map table to class
    CidMintingStore = Base.classes.cid_minting_store
    return {
        'engine': engine, 
        'session': session, 
        'table': CidMintingStore}

def find_all(CidMintingStore, session):
    query = session.query(CidMintingStore)
    return query.all()

def find_by_identifier(CidMintingStore, session, data_type, value):
    query = session.query(CidMintingStore).filter(CidMintingStore.type==data_type).filter(CidMintingStore.identifier==value)

    record = query.first()
    return record

def find_query(engine, sql, params=None):
    with engine.connect() as connection:
        results = connection.execute(sql, params or ())
        results_dict = [dict(row) for row in results.fetchall()]
        return results_dict

def find_cids_by_ocns(engine, ocns_list):
    """Find matched CIDs by ocns
    Return: a dict with the following keys:
      'inquiry_ocns': list of inquiry ocns
      'matched_cids': list of cids
      'min_cid': the lowest cid in the matched list
      'num_of_cids': number of matched cids
    """
    matched_cids = {
        'inquiry_ocns': ocns_list,
        'matched_cids': [],
        'min_cid': None,
        'num_of_cids': 0
    }

    # Convert list item to a single quoted string, concat with a comma and space
    ocns = list_to_str(ocns_list)
    if valid_sql_in_clause_str(ocns):
        sql = "SELECT cid FROM cid_minting_store WHERE type='oclc' AND identifier IN (" + ocns + ")"
        results = find_query(engine, sql)
        if results:
            matched_cids['matched_cids'] = results
            matched_cids['min_cid'] = min([cid.get("cid") for cid in results])
            matched_cids['num_of_cids'] = len(results)

    return matched_cids

def insert_a_record(session, record):
    try:
        session.add(record)
        session.flush()
    except IntegrityError as e:
        session.rollback()
        print(e)
        logging.error("IntegrityError adding record")
        logging.info("type: {}, value: {}, cid: {} ".format(record.type, record.identifier, record.cid))
    else:
        session.commit()

def main():
    if (len(sys.argv) > 1):
        env = sys.argv[1]
    else:
        env = "test"

    configs= get_configs_by_filename('config', 'cid_minting')
    print(configs)
    
    logfile = configs[env]['logpath']
    db_config = str(utils.db_connect_url(configs[env]['minter_db']))

    logging.basicConfig(
            level=logging.DEBUG,
            filename=logfile,
            format="%(asctime)s %(levelname)-4s %(message)s",
        )
    logging.info("Start " + os.path.basename(__file__))

    DB_CONNECT_STR = os.environ.get('OVERRIDE_DB_CONNECT_STR') or db_config
    print("db_connect_str {}".format(DB_CONNECT_STR))

    db = prepare_database(DB_CONNECT_STR)
    engine = db['engine']
    session = db['session']
    CidMintingStore = db['table']

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
    insert_a_record(session, record)
    record = find_by_identifier(CidMintingStore, session, 'oclc', '30461866')
    if record:
        print("type: {}, value: {}, cid {} ".format(record.type, record.identifier, record.cid))

if __name__ == "__main__":
    main()
