import os
from os.path import join, dirname
import sys

# sqlalchemy version: 1.3.18 (legacy)
# CURRENT RELEASE: 1.4.39, Release Date: June 24, 2022
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import IntegrityError

import json
import logging

class LocalMinter:
    def __init__(self, db_connect_str):
        self._prepare_database(db_connect_str)

    def _prepare_database(self, db_connect_str):
        engine = create_engine(db_connect_str)
        Session = sessionmaker(engine)
        session = Session()

        Base = automap_base()
        # reflect the tables
        Base.prepare(engine, reflect=True)
        # map table to class
        tablename = Base.classes.cid_minting_store

        self.engine = engine
        self.session = session
        self.tablename = tablename

    def find_cid(self, data_type, identifier):
        results = {}
        record = self._find_record_by_identifier(data_type, identifier)
        if record:
            results['data_type'] = data_type
            results['inquiry_identifier'] = identifier
            results['matched_cid'] = record.cid
        return results

    def write_identifier(self, data_type, identifier, cid):
        record = self.tablename(type=data_type, identifier=identifier, cid=cid)
        if self._find_record(record):
            return "Record exists. No need to update"
        if self._find_record_by_identifier(data_type, identifier):
            if self._update_a_record(record):
                return "Updated an exsiting record"
        else:
            if self._insert_a_record(record):
                return "Inserted a new record"
        return None


    def _find_all(self):
        query = self.session.query(self.tablename)
        return query.all()

    def _find_record(self, record):
        """Find record from the cid_minting_store table by data type, identifier value and cid.
           The cid_minting_store table schema: (type, identifier, cid)
           Sample values:
           ("ocn", "8727632", "002492721"),
           ("sysid", "pur215476", "002492721")
        """
        query = self.session.query(self.tablename).filter(self.tablename.type==record.type, self.tablename.identifier==record.identifier, self.tablename.cid==record.cid)

        record = query.first()
        return record

    def _find_record_by_identifier(self, data_type, value):
        """Find record from the cid_minting_store table by data type and identifier value.
           The cid_minting_store table schema: (type, identifier, cid)
           Sample values:
           ("ocn", "8727632", "002492721"),
           ("sysid", "pur215476", "002492721")
        """
        query = self.session.query(self.tablename).filter(self.tablename.type==data_type).filter(self.tablename.identifier==value)

        record = query.first()
        return record

    def _insert_a_record(self, record):
        ret = None
        try:
            self.session.add(record)
            self.session.flush()
            ret = 1
        except Exception as e:
            self.session.rollback()
            #logging.error("IntegrityError adding record")
            #logging.info("type: {}, value: {}, cid: {} ".format(record.type, record.identifier, record.cid))
            #return "Database Error: failed to insert a record"
        else:
            self.session.commit()
        return ret

    def _update_a_record(self, record):
        ret = None
        try:
            ret = self.session.query(self.tablename).filter(self.tablename.type == record.type, self.tablename.identifier == record.identifier).update({self.tablename.cid: record.cid}, synchronize_session=False)
        except Exception as e:
            self.session.rollback()
            #logging.error("IntegrityError adding record")
            #logging.info("type: {}, value: {}, cid: {} ".format(record.type, record.identifier, record.cid))
        else:
            self.session.commit()
        return ret
