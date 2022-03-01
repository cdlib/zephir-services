## How to run TACT Transformer

### Process files from one publisher
* Copy input files to the indata/{publisher_name} directory
* Run the `tact_transformer.py' script in the tact_project folder

```
cd {tact project home directory}
python3 tact_transformer.py {publisher_name}
```

### Process files from multiple publishers 
* Copy input files to the indata/{publisher_name} directories
* Run the `tact_transformer.py' script in the tact_project folder

```
cd {tact project home directory}
python3 tact_transformer.py
```

## Outputs
Converted data entries are saved in output files and in the TACT database
* output files: {tact project home directory}/outputs/{publisher_name}
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
  * Run status: 'S'uccess or 'F'ailed
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
{"status": "S", 
"filename": "ACM_UC_Report_Input.csv", 
"error_msg": "", 
"publisher": "acm", 
"run_datetime": "2022-02-28 15:27:12.373632", 
"input_records": 685, 
"rejected_records": 0, 
"new_records_added": 685, 
"total_processed_records": 685, 
"existing_records_updated": 0}
``` 

## How to test

### Test data files
The test data files are in the `indata_testdata/{publisher}` directory, one file for each publisher under each publisher's sub-dir:

acm/ACM_UC_Report_Input.csv
cob/CoB_input_21_23.csv
csp/CSP_input.csv
cup/CUP_input_June2021.csv
elsevier/Elsevier_202107_Input.csv
plos/PLOS_2021_June.csv
springer/Springer_input_202106.csv
trs/Royal_Society_2021_07.csv

### Copy the directory and files to the `indata` directory:

rm -r indata
cp -r indata_testdata indata

### Run the transformer script

python3 tact_transformer.py

### Compare output files
Output files are in the `outputis/{publisher}` directory. A base output file for each publisher is save under the publisher's sub-dir with a `.base` extension.

acm/ACM_output.csv.base
cob/CoB_output.csv.base
csp/CSP_ouput.csv.base
cup/CUP_output.csv.base
elsevier/Elsevier_output.csv.base
plos/PLOS_output.csv.base
springer/Springer_output.csv.base
trs/Royal_Society_output.csv.base

Compare the output from recent run with the base output to make sure your code changes did affect the output. If yes, then provide a fix to either the code or the base output accordingly.

