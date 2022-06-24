import os
from os.path import join, dirname
import sys

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import IntegrityError

import logging

from cid_minter.zephir_cluster_lookup import list_to_str
from cid_minter.zephir_cluster_lookup import valid_sql_in_clause_str
from cid_minter.zephir_cluster_lookup import invalid_sql_in_clause_str


def prepare_database(db_connect_str):
    engine = create_engine(db_connect_str)
    Session = sessionmaker(engine)
    session = Session()

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
        sql = "SELECT cid FROM cid_minting_store WHERE type='ocn' AND identifier IN (" + ocns + ")"
        results = find_query(engine, sql)
        if results:
            matched_cids['matched_cids'] = results
            matched_cids['min_cid'] = min([cid.get("cid") for cid in results])
            matched_cids['num_of_cids'] = len(results)

    return matched_cids

def find_cid_by_sysid(CidMintingStore, session, sysid):
    results = {}
    record = find_by_identifier(CidMintingStore, session, 'sysid', sysid)
    if record:
        results['inquiry_sys_id'] = sysid 
        results['matched_cid'] = record.cid
    return results 

def insert_a_record(session, record):
    try:
        session.add(record)
        session.flush()
    except Exception as e:
        session.rollback()
        #logging.error("IntegrityError adding record")
        #logging.info("type: {}, value: {}, cid: {} ".format(record.type, record.identifier, record.cid))
        return "Database Error: failed to insert a record"
    else:
        session.commit()
        return "Success"

