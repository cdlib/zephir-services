"""Build a test concordance with the production mapping schema."""

import sqlite3
from pathlib import Path


def create_concordance_db(directory, dataframe):
    path = Path(directory) / "concordance.sqlite"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE mapping (variant INTEGER PRIMARY KEY, canonical INTEGER NOT NULL) STRICT")
        db.execute(
            "CREATE INDEX idx_map_canonical_variant ON mapping(canonical, variant) "
            "WHERE variant <> canonical"
        )
        db.executemany(
            "INSERT INTO mapping (variant, canonical) VALUES (?, ?)",
            ((int(row.ocn), int(row.primary)) for row in dataframe.itertuples()),
        )
    return str(path)
