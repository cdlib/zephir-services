"""Resolve OCLC numbers using the concordance SQLite mapping table."""

import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from functools import lru_cache

SQLITE_INTEGER_MAX = (1 << 63) - 1


def _sqlite_integer(value):
    """Convert an OCN to int, returning None when SQLite cannot bind it."""
    value = int(value)
    return value if value <= SQLITE_INTEGER_MAX else None


@contextmanager
def open_concordance(db_path):
    """Open an existing concordance database for read-only lookup."""
    if sqlite3.sqlite_version_info < (3, 37, 0):
        raise RuntimeError("SQLite 3.37 or newer is required for the concordance mapping table")
    path = Path(db_path).resolve(strict=True)
    db = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    try:
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='mapping'").fetchone() is None:
            raise ValueError(f"No mapping table in concordance database: {path}")
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_map_canonical_variant'").fetchone() is None:
            raise ValueError(f"No reverse lookup index in concordance database: {path}")
        yield db
    finally:
        db.close()


def get_primary_ocn(ocn, db_path, db=None):
    """Return the canonical OCN, or None when the OCN is absent."""
    if not ocn:
        return None
    ocn = _sqlite_integer(ocn)
    if ocn is None:
        return None
    if db is None:
        with open_concordance(db_path) as connection:
            return get_primary_ocn(ocn, db_path, connection)
    row = db.execute("SELECT canonical FROM mapping WHERE variant = ?", (ocn,)).fetchone()
    return row[0] if row else None


def get_ocns_cluster_by_primary_ocn(primary_ocn, db_path, db=None):
    """Return variants without the canonical OCN, or None for no variants."""
    if not primary_ocn:
        return None
    primary_ocn = _sqlite_integer(primary_ocn)
    if primary_ocn is None:
        return None
    if db is None:
        with open_concordance(db_path) as connection:
            return get_ocns_cluster_by_primary_ocn(primary_ocn, db_path, connection)
    variants = [row[0] for row in db.execute(
        "SELECT variant FROM mapping WHERE canonical = ? AND variant <> canonical ORDER BY variant",
        (primary_ocn,),
    )]
    return variants or None


def get_ocns_cluster_by_ocn(ocn, db_path, db=None):
    """Return all OCNs in a known cluster, including its canonical OCN."""
    if db is None:
        with open_concordance(db_path) as connection:
            return get_ocns_cluster_by_ocn(ocn, db_path, connection)
    primary = get_primary_ocn(ocn, db_path, db)
    if primary is None:
        return None
    variants = get_ocns_cluster_by_primary_ocn(primary, db_path, db)
    return (variants or []) + [primary]


class ConcordanceLookup:
    """Worker-owned read connection and bounded caches for a fixed concordance.

    Restart the owner after replacing or updating the concordance database.
    Zephir CID results are deliberately never cached.
    """

    def __init__(self, db_path):
        self.db_path = db_path
        self._context = None
        self._db = None
        self._primary = lru_cache(maxsize=8192)(self._get_primary)
        self._cluster = lru_cache(maxsize=1024)(self._get_cluster)

    def _connection(self):
        if self._db is None:
            context = open_concordance(self.db_path)
            self._db = context.__enter__()
            self._context = context
        return self._db

    def _get_primary(self, ocn):
        return get_primary_ocn(ocn, self.db_path, self._connection())

    def _get_cluster(self, primary):
        variants = get_ocns_cluster_by_primary_ocn(primary, self.db_path, self._connection())
        return tuple(sorted((variants or []) + [primary]))

    def get_clusters(self, ocns, timings=None):
        started = time.perf_counter()
        self._connection()
        opened = time.perf_counter()
        primaries = {self._primary(ocn) for ocn in set(ocns)}
        resolved = time.perf_counter()
        clusters = {self._cluster(primary) for primary in primaries if primary is not None}
        if timings is not None:
            timings["concordance_open_seconds"] = opened - started
            timings["concordance_primary_seconds"] = resolved - opened
            timings["concordance_clusters_seconds"] = time.perf_counter() - resolved
        return clusters

    def close(self):
        self._primary.cache_clear()
        self._cluster.cache_clear()
        if self._context is not None:
            self._context.__exit__(None, None, None)
            self._context = None
            self._db = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def get_clusters_by_ocns(ocns, db_path, lookup=None, timings=None):
    """Fetch each distinct canonical cluster once, optionally reusing a worker."""
    if not ocns:
        return set()
    if lookup is not None:
        return lookup.get_clusters(ocns, timings)
    with ConcordanceLookup(db_path) as lookup:
        return lookup.get_clusters(ocns, timings)


def convert_set_to_list(set_of_tuples):
    return [list(cluster) for cluster in set_of_tuples]


def lookup_ocns_from_oclc(ocns, concordance_db_path, lookup=None, timings=None):
    """Return the OCLC cluster result expected by CID inquiry callers."""
    clusters = convert_set_to_list(get_clusters_by_ocns(ocns, concordance_db_path, lookup, timings))
    return {
        "inquiry_ocns": ocns,
        "matched_oclc_clusters": clusters,
        "num_of_matched_oclc_clusters": len(clusters),
    }
