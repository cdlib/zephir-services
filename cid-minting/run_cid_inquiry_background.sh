#!/bin/bash
# Retrieve Zephir clusters by OCNs.
# Script name: run_cid_inquiry.sh
# Arguments:
#   arg1: Comma separated OCNs in integer without spaces in between any two values
# Usage: run_cid_inquiry.sh comma_separated_ocns
# For example: run_cid_inquiry.sh 1,6567842,6758168,8727632
# Created: 8/6/2020. 

# environment: dev, stg, or prd
ENV=`uname -n| cut -d '-' -f3`

PIPFILE="/apps/htmm/zephir-services/Pipfile"
SCRIPT="/apps/htmm/zephir-services/cid-minting/cid_inquiry.py"

if [ ! -e $SCRIPT ]; then
  echo "Script $SCRIPT does not exist."
  exit 1
fi

cmd="pipenv run python $SCRIPT $ENV $OCNS"

PIPENV_PIPFILE=$PIPFILE $cmd &

exit $? 

