## How to run TACT Transformer

### Process files from one publisher
* Copy input files to the indata/{publisher_name} directory
* Run the `tact_transformer.py' script in the tact_project home directory

```
cd {TACT_HOME}
python3 tact_transformer.py {publisher_name}
```

### Process files from multiple publishers 
* Copy input files to the indata/{publisher_name} directories
* Run the `tact_transformer.py' script in the tact_project home directory

```
cd {TACT_HOME}
python3 tact_transformer.py
```

## Outputs
Converted data entries are saved in output files and in the TACT database
* output files: {TACT_HOME}/outputs/{publisher_name}
* database table for output: publisher_reports

## Database
* Database servers:
  * Dev: rds-col-tact-dev.cmcguhglinoa.us-west-2.rds.amazonaws.com
  * Prd: rds-col-tact-prd.cmcguhglinoa.us-west-2.rds.amazonaws.com
* Database name: tact
* User accounts:
  * tactdba: dba account
  * tactrw: account with read & write privileges 
  * tactro: read only account
* Database tables:
  * publisher_reports: new and updated entries in converted/transformed format
  * transaction_log: new, updated and rejected entries in converted/transformed format
  * run_reports: summary run report including

## Run Reports
Run reports are saved in the log file and in the `run_reports` table in the TACT database
* Log file: logs/tact_run.log
* TACT database table: run_reports
* Run report includes: 
  * Run status:
    * S: Success
    * F: Failed
  * Filename
  * Publisher
  * Run date/time
  * Input records
  * Total processed records
  * Rejected records
  * New records added
  * Exisitng records updated

### Sample Run Report
```
{
"status": "S", 
"filename": "ACM_UC_Report_Input.csv", 
"error_msg": "", 
"publisher": "acm", 
"run_datetime": "2022-02-28 15:27:12.373632", 
"input_records": 685, 
"rejected_records": 0, 
"new_records_added": 685, 
"total_processed_records": 685, 
"existing_records_updated": 0
}
``` 
## Transaction Log
For each data entry it processed, the TACT Transformer saves the data entry together with the processing status in the transaction_log table. 
Transaction status includes:
* N: new data entry
* U: updates to exsiting data entry
* R: rejected data entry due to data errors

## Logs
{TACT_HOME}/logs/tact_run.log

## How to Deploy the Project

### Check out the code from git repository
Checkout the code from git repository into the `{TACT_HOME}` directory

### Install required packages
Run the `scripts/install_pacakges.sh` script to install required packages:
* git
* Python3
* MySQL

### Run the deployment script
Run the `scripts/deploy_tact.sh` script to:
* Create required directories
* Install required Python packages
  * Required packages are listed in {TACT_HOME}/requirements.txt
  * Run `pip install --user -r requirements.txt` command to install the packages

### Manually setup configuraiton file
Manually setup the configuraiton file tact_db.yml in the `config` directory under `$TACT_HOME`.
The tact_db.yml file contains:
```
host: rds-col-tact-prd.cmcguhglinoa.us-west-2.rds.amazonaws.com
port: 3306
username: tactrw 
password: <password>
database: tact
drivername: mysql+mysqlconnector
```

### Manually create database tables
Run the create table SQL scripts in the {TACT_HOME}/scripts directory to create the tables.
SQL scripts:
```
create_publisher_reports_table.sql
create_run_reports_table.sql
create_transaction_log_table.sql
```
Use the `tactdba` account to create the database tables. 
Sample SQL command:
```
mysql -h rds-col-tact-dev.cmcguhglinoa.us-west-2.rds.amazonaws.com -u tactdba tact -p{tactdba_password} < create_publisher_reports_table.sql 
```

## How to Test

### Test data files
Test data files are saved in the `indata_testdata/{publisher}` directory. One test file is prepared for each publisher under the publisher's sub-dir:

```
acm/ACM_UC_Report_Input.csv
cob/CoB_input_21_23.csv
csp/CSP_input.csv
cup/CUP_input_June2021.csv
elsevier/Elsevier_202107_Input.csv
plos/PLOS_2021_June.csv
springer/Springer_input_202106.csv
trs/Royal_Society_2021_07.csv
```

### Copy the directory and files to the `indata` directory:

rm -r indata
cp -r indata_testdata indata

### Run the transformer script

python3 tact_transformer.py

### Compare output files
Output files are in the `outputis/{publisher}` directory. A base output file for each publisher is save under the publisher's sub-dir with a `.base` extension.
```
acm/ACM_output.csv.base
cob/CoB_output.csv.base
csp/CSP_ouput.csv.base
cup/CUP_output.csv.base
elsevier/Elsevier_output.csv.base
plos/PLOS_output.csv.base
springer/Springer_output.csv.base
trs/Royal_Society_output.csv.base
```
Compare the output from recent run with the base output to make sure your code changes did affect the output. If yes, then provide a fix to either the code or the base output accordingly.

