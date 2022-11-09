# Prepare Zephir Records
The application validates contributor submitted MARC records, converts the records to Zephir MARCXML format and assigns a cluster ID to each record.
# Setup
1. Deploy zephir-service.git on the server 
   - Deploy directory: /apps/htmm/zephir-services
2. Compile or copy the Zephir database configuration file zephir_db.yml from dev or test server
   - Filename: /apps/htmm/zephir-services/prepare_zephir_records/config/zephir_db.yml
Sampe database configuration:
```
dev:
    host: rds-d2d-htmm-dev.cmcguhglinoa.us-west-2.rds.amazonaws.com
    port: 3306
    username: htmmrw 
    password: <password>
    database: htmm
    drivername: mysql+mysqlconnector
```
3. Verify CID minter configuration
   - The configuration file: /apps/htmm/zephir-services/prepare_zephir_records/config/cid_minting.yml 
   - Configuration entries:
     - logpath
     - Minter database name
     - LevelDB primary file path
     - LevelDB cluster file path
Sample configuraiton:
```
dev:
  minter_db:
    database:  /apps/htmm/minterdb/cid_minting_store.sqlite
    drivername: sqlite
  primary_db_path: /apps/htmm/leveldb/leveldb_files/primary-lookup 
  cluster_db_path: /apps/htmm/leveldb/leveldb_files/cluster-lookup

logpath: /apps/htmm/log/cid_minting/cid_minting.log
```
4. Create the SQLite cid_minting_store.sqlite database
   - Find the db creating script zephir-services.git/prepare_zephir_records/cid_minter/database/cid_minting_store.sqlite 
   - Create the cid_minting_store.sqlite with the defined full path `/apps/htmm/minterdb/cid_minting_store.sqlite`
```
sqlite3 /apps/htmm/minterdb/cid_minting_store.sqlite < create_cid_minting_store_table.sql
```
5. Vefiry LevelDB files - make sure they exist and up-to-date
   - /apps/htmm/leveldb/leveldb_files/primary-lookup
   - /apps/htmm/leveldb/leveldb_files/cluster-lookup

6. Verify the log `/apps/htmm/log/cid_minting/cid_minting.log` exists. If not create a new onw

7. Run tests in the following directories:
   - zephir-services/prepare_zephir_records
   - zephir-services/prepare_zephir_records/cid_minter
```
pipenv run pytest tests/
```
