#!/bin/bash
# script: run_integration_tests.sh 
#

echo "Prepare test environment..."
cd integration_tests
./prepare_test_environment.sh
cd ..

echo "Perform integration tests..."
PIPFILE="/apps/htmm/zephir-services/Pipfile"
SCRIPT="run_cid_minter.py"
id_file="./integration_tests/test_datasets/test_ids.json"
cmd="pipenv run python $SCRIPT test $id_file"

PIPENV_PIPFILE=$PIPFILE $cmd
