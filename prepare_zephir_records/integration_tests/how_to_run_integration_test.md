# How to run the integration tests

## Create a symbol link to the create_cid_minting_store_table.sql:

create_cid_minting_store_table.sql -> ../cid_minter/database/create_cid_minting_store_table.sql
```
ln -s ../cid_minter/database/create_cid_minting_store_table.sql create_cid_minting_store_table.sql
```

## How to run the integration tests

```
./run_integration_tests.sh <password_for_htmmdba>"
```
