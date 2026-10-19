#!/bin/bash
# script: prepare_test_environment.sh 
#
# This script creates test databases and populates datasets for integration tests.

echo "## Create the Zephir tstdev database"
./prepare_zephir_db.sh

echo "## Create SQLite concordance database for OCLC lookup"

PIPFILE="/apps/htmm/zephir-services/Pipfile"
SCRIPT="prepare_sqlite_concordance.py"
cmd="pipenv run python $SCRIPT"


PIPENV_PIPFILE=$PIPFILE $cmd
if [ $? -ne 0 ]; then
    echo "Command failed: '${cmd}'"
    exit 1
fi
