# Prepare Zephir Records
The application validates contributor submitted MARC records, converts the records to Zephir MARCXML format and assigns a cluster ID to each record.
# Setup
1. Deploy zephir-service.git on the server 
   - Deploy directory: /apps/htmm/zephir-services
2. Compile or copy the Zephir database configuration file zephir_db.yml from dev or test server
   - Filename: /apps/htmm/zephir-services/prepare_zephir_records/config/zephir_db.yml
   - Sample database configuration:
```
dev:
    host: rds-d2d-htmm-dev.cmcguhglinoa.us-west-2.rds.amazonaws.com
    port: 3306
    username: htmmrw 
    password: <password>
    database: htmm
    drivername: mysql+mysqlconnector
```
3. Verify CID minting configuration
   - The configuration file: /apps/htmm/zephir-services/prepare_zephir_records/config/cid_minting.yml 
   - Configuration entries:
     - logpath
     - LevelDB primary file path
     - LevelDB cluster file path
   - Sample configuraiton:
```
primary_db_path: /apps/htmm/leveldb/leveldb_files/primary-lookup 
cluster_db_path: /apps/htmm/leveldb/leveldb_files/cluster-lookup

logpath: /apps/htmm/log/cid_minting/cid_minting.log
```
4. Vefiry LevelDB files - make sure they exist and up-to-date
   - /apps/htmm/leveldb/leveldb_files/primary-lookup
   - /apps/htmm/leveldb/leveldb_files/cluster-lookup

5. Verify the log `/apps/htmm/log/cid_minting/cid_minting.log` exists. If not create a new one.

6. Run tests in the following directories:
   - zephir-services/prepare_zephir_records
   - zephir-services/prepare_zephir_records/cid_minter
```
pipenv run pytest tests/
```
# How to run the CID minter
The CID assignment script `assign_cid_to_zephir_records.py` assigns CIDs to records in a given Zephir records file.

* Directory:/apps/htmm/zephir-services/prepare_zephir_records
* Script: assign_cid_to_zephir_records.py

### To get help:
```
pipenv run python assign_cid_to_zephir_records.py -h
usage: assign_cid_to_zephir_records.py [-h] [--console] --env
                                       [{test,dev,stg,prd}] --source_dir
                                       [SOURCE_DIR] --target_dir [TARGET_DIR]
                                       --infile [INPUT_FILENAME]
                                       [--outfile [OUTPUT_FILENAME]]

Assign CID to Zephir records.

optional arguments:
  -h, --help            show this help message and exit
  --console, -c         display log entries on screen
  --env [{test,dev,stg,prd}], -e [{test,dev,stg,prd}]
                        define runtime environment
  --source_dir [SOURCE_DIR], -s [SOURCE_DIR]
                        source file directory
  --target_dir [TARGET_DIR], -t [TARGET_DIR]
                        target file directroy
  --infile [INPUT_FILENAME], -i [INPUT_FILENAME]
                        input filename
  --outfile [OUTPUT_FILENAME], -o [OUTPUT_FILENAME]
                        output filename
```

### To assign CIDs to Zephir recors in a file
Run the `assign_cid_to_zephir_records.py` script with in the `/apps/htmm/zephir-services/prepare_zephir_records` directory:
```
cd /apps/htmm/zephir-services/prepare_zephir_records

pipenv run python assign_cid_to_zephir_records.py -e dev -s soruce_file_dir -i input_filename -t target_file_dir -o output_filename(optional)
```
