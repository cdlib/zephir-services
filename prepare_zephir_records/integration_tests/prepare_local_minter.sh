

local_minter="local_minter/cid_minting_store.sqlite"

sql="create_cid_minting_store_table.sql"

sqlite3 ${local_minter} < $sql

sql="insert_data_cid_minting_store.sql"
sqlite3 ${local_minter} < $sql
