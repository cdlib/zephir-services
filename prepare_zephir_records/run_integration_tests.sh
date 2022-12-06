#!/bin/bash
# script: run_integration_tests.sh 
#

# TO-DO: review and update prepare_test_environment.sh
echo "Prepare test environment..."
cd integration_tests
./prepare_test_environment.sh
cd ..

echo "Perform integration tests..."
PIPFILE="/apps/htmm/zephir-services/prepare_zephir_records/Pipfile"
SCRIPT="assign_cid_to_zephir_records.py"
ENV="test"
src_dir=""
target_dir=""
input_file=""
output_file=""
cmd="pipenv run python $SCRIPT"

echo "Test case 1: define test case"
input_file="./integration_tests/test_datasets/test_file_1.xml"
option="-e ${ENV} -s ${src_dir} -t ${target_dir} -i ${input_file} -o ${output_file}"
ret="$(PIPENV_PIPFILE=$PIPFILE $cmd $option)"

#TO-DO: perform integration tests
#
