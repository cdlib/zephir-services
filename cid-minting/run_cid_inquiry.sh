#!/bin/bash
# Retrieve Zephir clusters by OCNs.
# Script name: run_cid_inquiry.sh
# Arguments:
#   arg1: Comma separated OCNs in integer without spaces in between any two values
# Usage: run_cid_inquiry.sh comma_separated_ocns
# For example: run_cid_inquiry.sh 1,6567842,6758168,8727632
# Created: 8/6/2020. 

function usage_error {
    echo $1
    echo "Usage: run_cid_inquiry.sh comma_separated_ocns"
    echo "For example: run_cid_inquiry.sh 1,6567842,6758168,8727632"
    exit 1
}

OCN_INSTRUCTION="Please provide comma separated OCNs in integer without spaces in between any two values."

if [ $# != 1 ]; then
    usage_error "Missing parameter. $OCN_INSTRUCTION"
fi

if ! [[ $1 =~ (^[1-9][0-9]*(,[1-9][0-9]*)*[0-9]*$) ]]; then
    usage_error "Parameter error. $OCN_INSTRUCTION"
fi

# environment: dev, stg, or prd
ENV=`uname -n| cut -d '-' -f3`

PIPFILE="/apps/htmm/zephir-services/Pipfile"
SCRIPT="/apps/htmm/zephir-services/cid-minting/cid_inquiry.py"
OCNS=$1

if [ ! -e $SCRIPT ]; then
  usage_error "Script $SCRIPT does not exist."
fi

cmd="pipenv run python $SCRIPT $ENV $OCNS"

PIPENV_PIPFILE=$PIPFILE $cmd

exit $? 

