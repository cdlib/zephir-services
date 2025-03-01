import os
import sys

from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import String
from sqlalchemy import Integer
from sqlalchemy import column
from sqlalchemy import table 
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError 

SELECT_TACT_BY_ID = "SELECT id, publisher, doi FROM publisher_reports WHERE id=:id"
SELECT_TACT_BY_PUBLISHER = "SELECT id, publisher, doi FROM publisher_reports WHERE publisher=:publisher"
SELECT_LAST_EDIT_BY_DOI = "SELECT last_edit FROM publisher_reports WHERE doi=:doi"
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


class TactDbTables:
    def __init__(self, database):
        self.database = database
        self.table = None

    def insert(self, records):
        if records:
            self.database.insert(self.table, records)

    def insert_update_on_duplicate_key(self, records):
        if records:
            self.database.insert_update_on_duplicate_key(self.table, records)


class PublisherReportsTable(TactDbTables):
    def __init__(self, database):
        super(PublisherReportsTable, self).__init__(database)
        self.table = table("publisher_reports",
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

    def find_tact_publisher_reports_by_id(self, id):
        params = {"id": id}
        return self.database.findall(text(SELECT_TACT_BY_ID), params)

    def find_tact_publisher_reports_by_publisher(self, publisher):
        params = {"publisher": publisher}
        return self.database.findall(text(SELECT_TACT_BY_PUBLISHER), params)

    def find_last_edit_by_doi(self, doi):
        params = {"doi": doi}
        return self.database.findall(text(SELECT_LAST_EDIT_BY_DOI), params)

    def find_new_records(self, last_edit):
        params = {"last_edit": last_edit}
        return self.database.findall(text(SELECT_NEW_RECORDS), params)

    def find_updated_records(self, last_edit):
        params = {"last_edit": last_edit}
        return self.database.findall(text(SELECT_UPD_RECORDS), params)

class TransactionLogTable(TactDbTables):
    def __init__(self, database):
        super(TransactionLogTable, self).__init__(database)
        self.table = table("transaction_log",
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
                column("transaction_status_json"),
                column("filename")
            )


class RunReportsTable(TactDbTables):
    def __init__(self, database):
        super(RunReportsTable, self).__init__(database)
        self.table = table("run_reports", column("run_report"))

