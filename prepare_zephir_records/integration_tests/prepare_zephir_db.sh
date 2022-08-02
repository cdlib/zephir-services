#!/bin/bash
# script: prepare_zephir_db.sh 
#
# This script creates tables and populates test datasets in the Zephir devtst database

if [ $1 ] ; then
  pw=$1
else
  echo "Ussage: $0 password_for_htmmdba"
  exit
fi

database="devtst"
host="rds-d2d-htmm-dev.cmcguhglinoa.us-west-2.rds.amazonaws.com"
user="htmmdba"

echo "Creating Zephir tables in the test DB: devtst"
sql_script="create_zephir_tables.sql"
mysql -h ${host} -u ${user} ${database} -p${pw} < ${sql_script} 

echo "Creating OCLC Xref table in the test DB: devtst"
sql_script="create_oclc_ref_table.sql"
mysql -h ${host} -u ${user} ${database} -p${pw} < ${sql_script} 

echo "Creating Zephir test DB: devtst - Complete!"
