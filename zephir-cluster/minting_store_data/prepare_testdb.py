"""prepare_testdb.py: create database and collection for testing 
   arg_1: database name (optional, default="testdb")
   arg_2: collection name (optional, default="cluster")
   prepare_testdb.py <database_name> <collection_name> 
"""
import sys

import pymongo
import json

database = "testdb"
collection = "cluster"

if len(sys.argv) >= 2:
    database = sys.argv[1]

if len(sys.argv) >= 3:
    collection = sys.argv[2]

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

dblist = myclient.list_database_names()
if database in dblist:
  print("The {} database exists.".format(database))

mydb = myclient[database]
collist = mydb.list_collection_names()

if collection in collist:
  print("The {} collection exists. Drop and Re-create".format(collection))
  mydb[collection].drop()

mycol = mydb[collection]

# insert from json file
idList = []
with open('cid_ids.json') as f:
    for jsonObj in f:
        idList.append(json.loads(jsonObj))

x = mycol.insert_many(idList)

#print list of the _id values of the inserted documents:
print(x.inserted_ids)

myclient.close()

