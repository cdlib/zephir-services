import os
import sys

from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import String
from sqlalchemy import Integer
#from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import table 
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError 

SELECT_TACT_BY_ID = "SELECT id, publisher, doi FROM publisher_reports WHERE id=:id"
SELECT_TACT_BY_PUBLISHER = "SELECT id, publisher, doi FROM publisher_reports WHERE publisher=:publisher"
SELECT_LAST_EDIT = "SELECT max(last_edit) as last_edit FROM publisher_reports"
SELECT_NEW_RECORDS = "SELECT doi from publisher_reports where last_edit = create_date AND last_edit>:last_edit"
SELECT_UPD_RECORDS = "SELECT * from publisher_reports where last_edit > create_date AND last_edit>:last_edit"

class Database:
    def __init__(self, db_connect_str):
        self.engine = create_engine(db_connect_str)

    def findall(self, sql, params=None):
        with self.engine.connect() as conn:
            try:
                results = conn.execute(sql, params or ())
                results_dict = [dict(row) for row in results.fetchall()]
                return results_dict
            except SQLAlchemyError as e:
                print("DB error: {}".format(e))
                return None
            

    def insert(self, db_table, records):
        """insert multiple records to a db table
           Args:
               db_table: table name in string
               records: list of records in dictionary
            Returns: None
               Idealy the number of affected rows. However sqlalchemy does not support this feature.
               The CursorResult.rowcount suppose to return the number of rows matched, 
               which is not necessarily the same as the number of rows that were actually modified.
               However, the result.rowcount here always returns -1.
        """ 
        with self.engine.connect() as conn:
            for record in records:
                try:
                    insert_stmt = insert(db_table).values(record)
                    result = conn.execute(insert_stmt)
                    print("{}".format(result.rowcount()))
                except SQLAlchemyError as e:
                    print("DB insert error: {}".format(e))

    def insert_update_on_duplicate_key(self, db_table, records):
        """insert multiple records to a db table
           insert when record is new
           update on duplicate key - update only when the content is changed 
           Args:
               db_table: table name in string
               records: list of records in dictionary
            Returns: None
               Idealy the number of affected rows. However sqlalchemy does not support this feature.
               The CursorResult.rowcount suppose to return the number of rows matched, 
               which is not necessarily the same as the number of rows that were actually modified.
               However, the result.rowcount here always returns -1.
        """
        with self.engine.connect() as conn:
            for record in records:
                try:
                    insert_stmt = insert(db_table).values(record)
                    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(record)
                    result = conn.execute(on_duplicate_key_stmt)
                except SQLAlchemyError as e:
                    print("DB insert error: {}".format(e))

    def close(self):
        self.engine.dispose()

def init_database(db_connect_str):
    return Database(db_connect_str)

def find_records(database, select_query, params=None):
    """
    Args:
        db_connect_str: database connection string
        sql_select_query: SQL select query
    Returns:
        list of dict with selected field names as keys
    """
    if select_query:
        records = database.findall(text(select_query), params)
        return records


def find_tact_publisher_reports_by_id(database, id):
    params = {"id": id}
    return find_records(database, SELECT_TACT_BY_ID, params)

def find_tact_publisher_reports_by_publisher(database, publisher):
    params = {"publisher": publisher}
    return find_records(database, SELECT_TACT_BY_PUBLISHER, params)

def find_last_edit_timestamp(database):
    return find_records(database, SELECT_LAST_EDIT)

def find_new_records(database, last_edit):
    params = {"last_edit": last_edit}
    return find_records(database, SELECT_NEW_RECORDS, params)

def find_updated_records(database, last_edit):
    params = {"last_edit": last_edit}
    return find_records(database, SELECT_UPD_RECORDS, params)

def insert_tact_publisher_reports(database, records):
    """Insert records to the publisher_reports table
    Args:
        database: a Database object
        records: list of dictionaries
    """
    if records:
        database.insert_update_on_duplicate_key(define_publisher_reports_table(), records)
        database.close

def insert_tact_transaction_log(database, records):
    """Insert records to the transaction_log table
    Args:
        database: a Database object
        records: list of dictionaries
    """
    if records:
        database.insert_update_on_duplicate_key(define_transaction_log_table(), records)
        database.close


def define_publisher_reports_table():
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

def define_transaction_log_table():
    return table("transaction_log",
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
      column("publisher_status"),
      column("transaction_status")
      )

