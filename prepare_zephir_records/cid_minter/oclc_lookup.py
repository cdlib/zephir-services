"""Resolve OCLC numbers using the concordance SQLite mapping table."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path


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
    if db is None:
        with open_concordance(db_path) as connection:
            return get_primary_ocn(ocn, db_path, connection)
    row = db.execute("SELECT canonical FROM mapping WHERE variant = ?", (ocn,)).fetchone()
    return row[0] if row else None


def get_ocns_cluster_by_primary_ocn(primary_ocn, db_path, db=None):
    """Return variants without the canonical OCN, or None for no variants."""
    if not primary_ocn:
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


def get_clusters_by_ocns(ocns, db_path):
    """Return deduplicated clusters as sets of sorted OCN tuples."""
    if not ocns:
        return set()
    with open_concordance(db_path) as db:
        clusters = set()
        for ocn in ocns:
            cluster = get_ocns_cluster_by_ocn(ocn, db_path, db)
            if cluster:
                clusters.add(tuple(sorted(cluster)))
        return clusters


def convert_set_to_list(set_of_tuples):
    return [list(cluster) for cluster in set_of_tuples]


def lookup_ocns_from_oclc(ocns, concordance_db_path):
    """Return the OCLC cluster result expected by CID inquiry callers."""
    clusters = convert_set_to_list(get_clusters_by_ocns(ocns, concordance_db_path))
    return {
        "inquiry_ocns": ocns,
        "matched_oclc_clusters": clusters,
        "num_of_matched_oclc_clusters": len(clusters),
    }
