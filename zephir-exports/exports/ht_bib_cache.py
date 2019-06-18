#!/usr/bin/env python

import datetime
import os
import zlib

import json
import mysql.connector

from lib.export_cache import ExportCache
from lib.vufind_formatter import VufindFormatter
import lib.utils as utils


def ht_bib_cache(
    console, input_path=None, cache_path=None, merge_version=None, force=False
):
    debug_start_time = datetime.datetime.now()

    # LOAD: environment, configuration
    default_root_dir = os.path.join(os.path.dirname(__file__), "..")
    APP = utils.AppEnv(name="ZEPHIR", root_dir=default_root_dir)
    console.debug("Loading application environment and configuration")
    console.debug("Environment: {}".format(APP.ENV))
    console.debug("Configuration: {}".format(APP.CONFIG_PATH))

    # CACHE (store for caclulated/merged records for exports)
    cache_path = cache_path or APP.CACHE_PATH
    # use working directory if relative path given
    if not os.path.isabs(cache_path):
        cache_path = os.path.join(os.getcwd(), cache_path)

    # create a template file name if only a directory is given
    if os.path.isdir(cache_path):
        cache_template = "cache-{}-{}.db".format(
            merge_version, datetime.datetime.today().strftime("%Y-%m-%d")
        )
        cache_path = "{}/{}".format(cache_path, cache_template)
        console.debug("Cache template for file:{}".format(cache_template))

    # if the directory doesn't exist, faile
    if not os.path.exists(os.path.dirname(cache_path)):
        console.error("Cache path invalid")
        raise SystemExit(2)

    #  create temporary cache
    tmp_cache_template = "tmp-cache-{}-{}".format(
        merge_version, datetime.datetime.today().strftime("%Y-%m-%d_%H%M%S.%f")
    )
    tmp_cache_path = os.path.join(os.path.dirname(cache_path), tmp_cache_template)
    console.debug("Tmp cache location: {}".format(tmp_cache_path))
    console.debug("Cache location: {}".format(cache_path))

    # create cache
    console.debug("Creating cache file, session")
    cache = ExportCache(path=tmp_cache_path, force=force)
    bulk_session = cache.session()

    try:
        # DATABASE: Access to current records
        # Load database settings
        db_settings = APP.CONFIG.get("database", {}).get(APP.ENV)
        db_config = utils.DatabaseHelper(config=db_settings, env_prefix="ZEPHIR")
        db_connection = mysql.connector.connect(**db_config.connection_args())
        db_cursor = db_connection.cursor()

        # Load merge version queries
        sql_select = {
            "v2": "select cid, db_updated_at, metadata_json, "
            "var_usfeddoc, var_score, concat(cid,'_',zr.autoid) as vufind_sort  "
            "from zephir_records as zr "
            "inner join zephir_filedata as zf on zr.id = zf.id "
            "where attr_ingest_date is not null "
            "order by cid, var_score DESC, vufind_sort ASC",
            "v3": "select cid, db_updated_at, metadata_json, "
            "var_usfeddoc, var_score, concat(cid,'_',zr.autoid) as vufind_sort  "
            "from zephir_records as zr "
            "inner join zephir_filedata as zf on zr.id = zf.id "
            "where attr_ingest_date is not null "
            "order by cid, var_usfeddoc DESC, var_score DESC, vufind_sort ASC",
        }

        # Execute query
        db_cursor.execute(sql_select[merge_version])

        # PROCESS: calculate/merge records from database into cache datastore
        console.debug("Processing records...")
        curr_cid = None
        records = []
        entries = []
        max_date = None
        for idx, row in enumerate(db_cursor):
            cid, db_date, record, var_usfeddoc, var_score, vufind_sort = row
            if cid != curr_cid or curr_cid is None:
                # write last cluster
                if curr_cid:
                    cache_id = curr_cid
                    cache_data = json.dumps(
                        VufindFormatter.create_record(curr_cid, records).as_dict(),
                        separators=(",", ":"),
                    )
                    cache_key = zlib.crc32(
                        "{}{}".format(len(records), max_date).encode("utf8")
                    )
                    cache_date = max_date
                    entry = cache.entry(cache_id, cache_key, cache_data, cache_date)
                    entries.append(entry)

                # prepare next cluster
                curr_cid = cid
                records = [record]
                max_date = db_date
            else:
                if db_date > max_date:
                    max_date = db_date
                records.append(record)

            # periodic save records to datastore to chunk work
            if idx % 5000 == 0:
                bulk_session.bulk_save_objects(entries)
                entries = []

        # finish processing on last chunk of work
        cache_id = curr_cid
        cache_data = json.dumps(
            VufindFormatter.create_record(curr_cid, records).as_dict(),
            separators=(",", ":"),
        )
        cache_key = zlib.crc32("{}{}".format(len(records), max_date).encode("utf8"))
        cache_date = max_date
        entry = cache.entry(cache_id, cache_key, cache_data, cache_date)
        entries.append(entry)
        bulk_session.bulk_save_objects(entries)

        console.debug("Finished processing, final commit to cache datastore")
        bulk_session.commit()
        bulk_session.close()
        os.rename(tmp_cache_path, cache_path)
    finally:
        # TODO(cc): This will fail if cursor not defined
        db_cursor.close()
        db_connection.close()

    console.debug(
        "Completed Cache: {}".format(str(datetime.datetime.now() - debug_start_time))
    )

    return cache_path


if __name__ == "__main__":
    ht_bib_cache()
