from contextlib import contextmanager
import datetime
import math
import os
import socket
import zlib


from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.ext.automap import automap_base


class ExportCache:
    def __init__(self, cache_dir, name):
        self.name = name
        self.engine = create_engine(
            "sqlite:///{}".format(os.path.join(cache_dir, "{}.db".format(name))),
            echo=False,
        )
        self.cache_schema_exists_on_load = self.engine.dialect.has_table(
            self.engine, "cache"
        )

        if not self.cache_schema_exists_on_load:
            with self.engine.connect() as con:
                create_table_stmt = "create table 'cache' (" \
                "'cache_id' TEXT,'cache_key' TEXT,'cache_data' TEXT," \
                "'cache_date' TEXT,'data_key' TEXT, 'data_date' TEXT, " \
                "PRIMARY KEY('cache_id'));"
                con.execute(create_table_stmt)
                create_index_stmt = "create index 'cache_id_key_index' ON " \
                "'cache' ('cache_id'	ASC,'cache_key');"
                con.execute(create_index_stmt)

        metadata = MetaData()
        metadata.reflect(self.engine, only=["cache"])
        # Table('cache', metadata, autoload=True, autoload_with=self.engine)
        Base = automap_base(metadata=metadata)
        Base.prepare()

        # mapped classes are now created with names by default
        # matching that of the table name.
        self.Cache = Base.classes.cache
        # Create a session to the database.
        session_factory = sessionmaker()
        session_factory.configure(bind=self.engine)
        self.Session = scoped_session(session_factory)

    @contextmanager
    def session_context(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def _load_index(self):
        cache_index = {}
        with self.engine.connect() as con:
            result = con.execute("select cache_id, cache_key from cache")
            for row in result:
                cache_index[row.cache_id] = row.cache_key
        return cache_index

    def _chunk_set(self, prechunk_set, limit):
        mod = math.ceil(len(prechunk_set) / limit)
        chunks = [set() for i in range(mod)]
        for idx, item in enumerate(prechunk_set):
            chunks[idx % mod].add(item)
        return chunks

    def remove_set(self, ids):
        id_sets = self._chunk_set(ids, 1000)
        with self.engine.connect() as con:
            for id_set in id_sets:
                con.execute(
                    "delete from cache where cache_id in ({})".format(
                        ", ".join('"{}"'.format(id) for id in id_set)
                    )
                )

    def session(self):
        return self.Session()

    def add(self, cache_id, cache_key, cache_data, cache_date):
        new_cache = self.entry(cache_id, cache_key, cache_data, cache_date)
        with self.session_context() as session:
            session.add(new_cache)

    def entry(self, cache_id, cache_key, cache_data, cache_date):
        d_key = zlib.crc32(cache_data.encode('utf8'))
        compressed_cache_data = zlib.compress(cache_data.encode('utf8'))
        new_cache = self.Cache(
            cache_id=cache_id,
            cache_key=cache_key,
            cache_data=compressed_cache_data,
            # cache_data = cache_data,
            cache_date=cache_date,
            data_key=d_key,
            data_date=str(datetime.datetime.utcnow())
        )
        return new_cache

    def update(self, cache_id, cache_key, cache_data, cache_date):
        with self.session_context() as session:
            cache = session.query(self.Cache).get(cache_id)
            cache.cache_key = cache_key
            cache.cache_date = cache_date
            # only update the data fields if the data has updated
            # note: the metadata changes indicated by a cache_key changes
            # will not always cause the data to change.
            data_key = str(zlib.crc32(cache_data.encode('utf8')))
            if cache.data_key != data_key:
                compressed_cache_data = zlib.compress(cache_data.encode('utf8'))
                cache.cache_data = compressed_cache_data
                # cache.cache_data = cache_data
                cache.data_key = data_key
                cache.data_date = str(datetime.datetime.utcnow())

    def get(self, cache_id):
        with self.session_context() as session:
            cache = session.query(self.Cache).get(cache_id)
            if cache:
                return {"cache_id":cache.cache_id,
                "cache_key":cache.cache_key,
                "cache_data":cache.cache_data,
                "cache_date":cache.cache_date,
                "data_key":cache.data_key,
                "data_date":cache.data_date,}


    def size(self):
        with self.session_context() as session:
            size = session.query(self.Cache).count()
        return size

    def compare(self, compare_index):
        cache_index = self._load_index()
        comparison = CacheComparison(cache_index, compare_index)
        return comparison


class CacheComparison:
    def __init__(self, cache_index, compare_index):
        self.cache_index = cache_index.copy()
        self.compare_index = compare_index.copy()
        self.stale = set()
        self.uncached = set()
        self.unexamined = set()
        self.verified = set()
        for cache_id in cache_index.keys():
            self.unexamined.add(cache_id)
        for cache_id, cache_key in self.compare_index.items():
            result = self.cache_index.get(cache_id, None)
            if result == None:
                self.uncached.add(cache_id)
            elif result != cache_key:
                self.stale.add(cache_id)
                self.unexamined.remove(cache_id)
            elif result == cache_key:
                self.verified.add(cache_id)
                self.unexamined.remove(cache_id)
