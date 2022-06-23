import os
import sys

from cid_minter.oclc_lookup import lookup_ocns_from_oclc
from cid_minter.zephir_cluster_lookup import ZephirDatabase
from cid_minter.cid_inquiry_by_ocns import cid_inquiry_by_ocns

class CidMinter:
    def __init__(self, config, ids):
        self.config = config
        self.ids = ids 

    def mint_cid(self):
        ocns = self.ids.get("ocns")
        zephirDb = ZephirDatabase(self.config.get("zephirdb_connect_str"))
        leveldb_primary_path = self.config.get("leveldb_primary_path")
        leveldb_cluster_path = self.config.get("leveldb_cluster_path")

        results = cid_inquiry_by_ocns(ocns, zephirDb, leveldb_primary_path, leveldb_cluster_path) 
        print(results)
        return results['min_cid']


