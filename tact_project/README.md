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

cp -r indata_testdata indata

### Run the transformer script

pipenv run python tact_transformer.py

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

