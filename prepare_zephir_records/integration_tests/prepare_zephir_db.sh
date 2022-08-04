#!/bin/bash
# script: prepare_zephir_db.sh 
#
# This script creates tables and populates test datasets in the Zephir devtst database

sql_config="zephir_devtst.config"
sql_cmd="mysql --defaults-file=${sql_config}"

echo "Creating Zephir tables in the test DB: devtst"
sql_script="create_zephir_tables.sql"
$sql_cmd < ${sql_script}
if [ $? -ne 0 ]; then
    echo "Command failed: '${sql_cmd} < ${sql_script}'"
    exit
fi

echo "Creating OCLC Xref table in the test DB: devtst"
sql_script="create_oclc_ref_table.sql"
$sql_cmd < ${sql_script}
if [ $? -ne 0 ]; then
    echo "Command failed: '${sql_cmd} < ${sql_script}'"
    exit
fi

echo "Creating Zephir test DB: devtst - Complete!"
