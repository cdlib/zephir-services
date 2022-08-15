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
ENV="test"
cmd="pipenv run python $SCRIPT"

echo "Test case 1: missing required HTID field"
id_file="./integration_tests/test_datasets/test_case_1.json"
ret="$(PIPENV_PIPFILE=$PIPFILE $cmd $ENV $id_file)"

if [[ "$ret" == *"missing required htid"* ]]; then
  echo "Test case 1: PASS"
else
  echo "Test case 1: FAIL"
fi

