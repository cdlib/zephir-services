#!/usr/bin/env python

import datetime
import os
import socket

from environs import Env
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import yaml

from lib.export_cache import ExportCache
from lib.utils import zephir_config

print(datetime.datetime.time(datetime.datetime.now()))

# APPLICATION SETUP
# load environment
env = Env()
env.read_env()

# load configuration files
config = zephir_config(
    env("ZEPHIR_ENV", socket.gethostname()).lower(),
    os.path.join(os.path.dirname(__file__), "config"),
)


def main():
    htmm_db = config["database"][config["env"]]

    HTMM_DB_CONNECT_STR = str(
        URL(
            htmm_db.get("drivername", None),
            htmm_db.get("username", None),
            htmm_db.get("password", None),
            htmm_db.get("host", None),
            htmm_db.get("port", None),
            htmm_db.get("database", None),
        )
    )
    htmm_engine = create_engine(HTMM_DB_CONNECT_STR)
    live_statement = (
        "select cid as cache_id, "
        "crc32(CONCAT(count(id), max(db_updated_at))) as cache_key "
        "from zephir_records "
        "where attr_ingest_date is not null "
        "group by cache_id"
    )

    print(datetime.datetime.time(datetime.datetime.now()))
    cache = ExportCache(os.path.abspath("cache"), "quick-complete")
    live_index = {}
    with htmm_engine.connect() as con:
        result = con.execute(live_statement)
        for row in result:
            live_index[row.cache_id] = row.cache_key
    comparison = cache.compare(live_index)
    print("uncached")
    print(len(comparison.uncached))
    print("verified")
    print(len(comparison.verified))
    print("stale")
    print(len(comparison.stale))
    print("unexamined")
    print(len(comparison.unexamined))
    print(datetime.datetime.time(datetime.datetime.now()))
    # cache.remove_set(comparison.unexamined)
    # updated_comp = cache.compare(live_index)
    # print("updated unexamined")
    # print(len(updated_comp.unexamined))


if __name__ == "__main__":
    main()
