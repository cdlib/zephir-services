#!/bin/bash
# script: create_zephir_test_db.sh 
#
# This script creates tables and populates test datasets in the Zephir devtst database

database="devtst"
host="rds-d2d-htmm-dev.cmcguhglinoa.us-west-2.rds.amazonaws.com"
user="htmmdba"
pw="htmmdba3dev"

echo "Creating Zephir tables in the test DB: devtst"
sql_script="create_zephir_tables.sql"
mysql -h ${host} -u ${user} ${database} -p${pw} < ${sql_script} 

echo "Create OCLC Xref table in the test DB: devtst"
sql_script="create_oclc_ref_table.sql"
mysql -h ${host} -u ${user} ${database} -p${pw} < ${sql_script} 

echo "Preparing Zephir test DB: devtst - Complete!"
