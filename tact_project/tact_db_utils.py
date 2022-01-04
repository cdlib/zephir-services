import os
import sys

from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import String
from sqlalchemy import Integer
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import table 
from sqlalchemy import insert 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError 

SELECT_TACT_BY_ID = "SELECT id, publisher, doi FROM transactions WHERE id=:id"

Base = declarative_base()

class Transactions(Base):
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True)
    publisher = Column(String(50))
    doi = Column(String(50))
    article_title = Column(String(150))
    corresponding_author = Column(String(50))
    corresponding_author_email = Column(String(50))
    uc_institution = Column(String(50))
    institution_identifier = Column(String(50))
    document_type = Column(String(50))
    eligible = Column(String(50))
    inclusion_date = Column(String(50))
    uc_approval_date = Column(String(50))
    article_access_type = Column(String(50))
    article_license = Column(String(50))
    journal_name = Column(String(50))
    issn_eissn = Column(String(50))
    journal_access_type = Column(String(50))
    journal_subject = Column(String(50))
    grant_participation = Column(String(50))
    funder_information = Column(String(150))
    full_coverage_reason = Column(String(150))
    original_apc_usd = Column(String(50))
    contractual_apc_usd = Column(String(50))
    library_apc_portion_usd = Column(String(50))
    author_apc_portion_usd = Column(String(50))
    payment_note = Column(String(150))
    cdl_notes = Column(String(150))
    license_chosen = Column(String(50))
    journal_bucket = Column(String(50))
    agreement_manager_profile_name = Column(String(50))
    publisher_status = Column(String(50))

class Database:
    def __init__(self, db_connect_str):
        self.engine = create_engine(db_connect_str)

    def findall(self, sql, params=None):
        with self.engine.connect() as conn:
            results = conn.execute(sql, params or ())
            results_dict = [dict(row) for row in results.fetchall()]
            return results_dict

    def insert(self, db_table, values):
        """insert multiple rows to a db table
        """ 
        with self.engine.connect() as conn:
            print(values)
            #result = conn.execute(db_table.insert().values(values))
            result = conn.execute(insert(db_table, values))


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


def find_tact_transactions_by_id(db_connect_str, id):
    params = {"id": id}
    return find_records(db_connect_str, SELECT_TACT_BY_ID, params)


def insert_tact_transactions(db_connect_str, values):
    """Insert rows to the transactions table
    Args:
        db_connect_str: database connection string
        values: list of dictionary
    """
    if values:
        try:
            db = Database(db_connect_str)
            db.insert(get_transactions_table(), values)
            db.close
        except SQLAlchemyError as e:
            print("DB insert error: {}".format(e))

def get_transactions_table():
    return table("transactions",
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
