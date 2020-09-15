#!/bin/bash
# 
# Script name: run_local_cid_minter.sh 
# Arguments:
# argv1: Action. Can only be 'read' or 'write'
# argv2: Data type. Can only be 'ocn' and 'sysid'
# argv3: Data. OCNs or a local system ID.
#     OCNs format:
#       Comma separated OCNs in integer without spaces in between any two values.
#       For example: 8727632,32882115
#     Local system ID: a string.
# argv4: A CID. Only required when Action='write'
#
# Usage: run_local_cid_minter.sh action[read|write] type[ocn|sysid] data[ocns|sysid] cid(required when action=write) 
# For example: 
#    run_local_cid_minter.sh read ocn 8727632,32882115
#    run_local_cid_minter.sh read sysid uc1234567
#    run_local_cid_minter.sh write ocn 30461866 011323406
#    run_local_cid_minter.sh write sysid uc1234567 011323407 
#
# Created: 9/13/2020. 

function usage_error {
    echo $1
    echo "Usage: run_local_cid_minter.sh action[read|write] type[ocn|sysid] data[ocn(s)|sysid] cid(required when action=write)"
    echo "For example:"
    echo "run_local_cid_minter.sh read ocn 8727632,32882115"
    echo "run_local_cid_minter.sh read sysid uc1234567"
    echo "run_local_cid_minter.sh write ocn 30461866 011323406"
    echo "run_local_cid_minter.sh write sysid uc1234567 011323407"
    exit 1
}

OCN_INSTRUCTION="Please provide comma separated OCNs in integer without spaces in between any two values."

if [[ $# != 3 && $# != 4 ]]; then
    usage_error "Parameter error."
fi

if [[ $1 != "read" && $1 != "write" ]]; then
    usage_error "Parameter error."
fi

if [[ $2 != "ocn" && $2 != "sysid" ]]; then
    usage_error "Parameter error."
fi

if [[ $2 == 'ocn' ]]; then
  if ! [[ $3 =~ (^[1-9][0-9]*(,[1-9][0-9]*)*[0-9]*$) ]]; then
    usage_error "Parameter error. $OCN_INSTRUCTION"
  fi
fi

action=$1
type=$2
data=$3
cid=""
if [ $# == 4 ]; then
  cid=$4
fi

# environment: dev, stg, or prd
ENV=`uname -n| cut -d '-' -f3`

LOG="/apps/htmm/log/cid_minting/cid_mint_store_run.log"

PIPFILE="/apps/htmm/zephir-services/Pipfile"
SCRIPT="/apps/htmm/zephir-services/cid-minting/local_cid_minter.py"

if [ ! -e $SCRIPT ]; then
  usage_error "Script $SCRIPT does not exist."
fi

echo `/bin/date` >>  $LOG

run_date=`/bin/date +%Y-%m-%d`

cmd="pipenv run python $SCRIPT $ENV $action $type $data $cid"
echo "cmd: $cmd" >> $LOG

PIPENV_PIPFILE=$PIPFILE $cmd

exit $? 

