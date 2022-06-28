1. Create the SQLite database cid_minting_store.sqlite when starting to use the cid_minting_store.py application

sqlite3 cid_minting_store.sqlite < create_cid_minting_store_table.sql

2. Config the cid_minting_store.py application to use the cid_minting_store.sqlite database

cid-minting/config/minter_db.yml 
dev:
    database: database/cid_minting_store.sqlite
    drivername: sqlite
 
