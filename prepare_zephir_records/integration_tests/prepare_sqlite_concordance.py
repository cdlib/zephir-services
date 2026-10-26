"""Build the SQLite concordance database used by integration tests."""

import sqlite3
from pathlib import Path

import pandas as pd


def main():
    data_file = Path("test_datasets/leveldb_test_datasets.csv")
    database_path = Path("test_datasets/concordance.sqlite")
    dataframe = pd.read_csv(data_file)

    database_path.unlink(missing_ok=True)
    with sqlite3.connect(database_path) as db:
        db.execute(
            "CREATE TABLE mapping ("
            "variant INTEGER PRIMARY KEY, canonical INTEGER NOT NULL"
            ") STRICT"
        )
        db.execute(
            "CREATE INDEX idx_map_canonical_variant ON mapping(canonical, variant) "
            "WHERE variant <> canonical"
        )
        db.executemany(
            "INSERT INTO mapping (variant, canonical) VALUES (?, ?)",
            ((int(row.ocn), int(row.primary)) for row in dataframe.itertuples()),
        )

    print(f"Created SQLite concordance database: {database_path.resolve()}")


if __name__ == "__main__":
    main()
