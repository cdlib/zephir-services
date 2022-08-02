#!/bin/bash
# script: prepare_local_minter.sh 
#
# This script creates tables and populates test datasets in the SQLite local minter database. 

local_minter="local_minter/cid_minting_store.sqlite"

echo "Local Minter: create the cid_minting_store table"
sql="create_local_cid_minting_store.sql"
sqlite3 ${local_minter} < $sql

echo "Creating Local Minter: Complete!"
