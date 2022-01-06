import os
import sys

from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import String
from sqlalchemy import Integer
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import table 
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError 

SELECT_TACT_BY_ID = "SELECT id, publisher, doi FROM publisher_reports WHERE id=:id"
SELECT_TACT_BY_PUBLISHER = "SELECT id, publisher, doi FROM publisher_reports WHERE publisher=:publisher"

class Database:
    def __init__(self, db_connect_str):
        self.engine = create_engine(db_connect_str)

    def findall(self, sql, params=None):
        with self.engine.connect() as conn:
            results = conn.execute(sql, params or ())
            results_dict = [dict(row) for row in results.fetchall()]
            return results_dict

    def insert(self, db_table, records):
        """insert multiple records to a db table
           Args:
               db_table: table name in string
               records: list of records in dictionary
        """ 
        with self.engine.connect() as conn:
            for record in records:
                try:
                    insert_stmt = insert(db_table).values(record)
                    result = conn.execute(insert_stmt)
                except SQLAlchemyError as e:
                    error = str(e.__dict__['orig'])
                    print("DB insert error: {}".format(error))

    def insert_update_on_duplicate_key(self, db_table, records):
        """insert multiple records to a db table
           insert when record is new
           update on duplicate key - update only when the content is changed 
           Args:
               db_table: table name in string
               records: list of records in dictionary
        """
        with self.engine.connect() as conn:
            for record in records:
                try:
                    insert_stmt = insert(db_table).values(record)
                    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(record)
                    result = conn.execute(on_duplicate_key_stmt)
                except SQLAlchemyError as e:
                    error = str(e.__dict__['orig'])
                    print("DB insert error: {}".format(error))

    def close(self):
        self.engine.dispose()

def find_records(db_connect_str, select_query, params=None):
    """
    Args:
        db_connect_str: database connection string
        sql_select_query: SQL select query
    Returns:
        list of dict with selected field names as keys
    """
    if select_query:
        try:
            db = Database(db_connect_str)
            records = db.findall(text(select_query), params)
            db.close()
            return records
        except SQLAlchemyError as e:
            print("DB Select error: {}".format(e))
            return None
    return None


def find_tact_publisher_reports_by_id(db_connect_str, id):
    params = {"id": id}
    return find_records(db_connect_str, SELECT_TACT_BY_ID, params)

def find_tact_publisher_reports_by_publisher(db_connect_str, publisher):
    params = {"publisher": publisher}
    return find_records(db_connect_str, SELECT_TACT_BY_PUBLISHER, params)

def insert_tact_publisher_reports(db_connect_str, records):
    """Insert records to the publisher_reports table
    Args:
        db_connect_str: database connection string
        records: list of dictionaries
    """
    if records:
        db = Database(db_connect_str)
        db.insert_update_on_duplicate_key(get_publisher_reports_table(), records)
        #db.insert(get_publisher_reports_table(), records)
        db.close

def get_publisher_reports_table():
    return table("publisher_reports",
      column("publisher"),
      column("doi"),
      column("article_title"),
      column("corresponding_author"),
      column("corresponding_author_email"),
      column("uc_institution"),
      column("institution_identifier"),
      column("document_type"),
      column("eligible"),
      column("inclusion_date"),
      column("uc_approval_date"),
      column("article_access_type"),
      column("article_license"),
      column("journal_name"),
      column("issn_eissn"),
      column("journal_access_type"),
      column("journal_subject"),
      column("grant_participation"),
      column("funder_information"),
      column("full_coverage_reason"),
      column("original_apc_usd"),
      column("contractual_apc_usd"),
      column("library_apc_portion_usd"),
      column("author_apc_portion_usd"),
      column("payment_note"),
      column("cdl_notes"),
      column("license_chosen"),
      column("journal_bucket"),
      column("agreement_manager_profile_name"),
      column("publisher_status")
      )

