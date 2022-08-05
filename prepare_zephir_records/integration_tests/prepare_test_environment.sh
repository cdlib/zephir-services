#!/bin/bash
# script: run_integration_tests.sh 
#
# This script creates test databases, populates test datasets, and performs integration tests.

echo "## Create the Zephir tstdev database"
./prepare_zephir_db.sh

echo "## Create the Local Minter"
./prepare_local_minter.sh

echo "## Create levelDB for OCLC lookup"

PIPFILE="/apps/htmm/zephir-services/Pipfile"
SCRIPT="prepare_leveldb.py"
cmd="pipenv run python $SCRIPT"

PIPENV_PIPFILE=$PIPFILE $cmd

