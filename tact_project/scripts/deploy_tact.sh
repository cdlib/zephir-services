#!/bin/bash

TACT_HOME="$HOME/zephir-services/tact_project/"
PUBLISHERS=(acm  cob  csp  cup  elsevier  jmir  plos  pnas  springer  trs)

echo "Create config and logs directories in $TACT_HOME"
mkdir -p $TACT_HOME/config $TACT_HOME/logs

echo ""
echo "Create indata, outputs and processed directories in $TACT_HOME"
for publisher in "${PUBLISHERS[@]}"
do
    mkdir -p $TACT_HOME/indata/$publisher
    mkdir -p $TACT_HOME/outputs/$publisher
    mkdir -p $TACT_HOME/processed/$publisher
done

echo ""
echo "install required Python packages"
pip install --user -r $TACT_HOME/requirements.txt

echo ""
echo "Please manually setup the configuraiton file tact_db.yml in $TACT_HOME/config"
echo "Please manually create the database tables using the create table SQL files in the $TACT_HOME/scripts directory"


