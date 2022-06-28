import os
import sys

from cid_minter.oclc_lookup import lookup_ocns_from_oclc
from cid_minter.zephir_cluster_lookup import ZephirDatabase
from cid_minter.cid_inquiry_by_ocns import cid_inquiry_by_ocns

class CidMinter:
    def __init__(self, config, ids):
        self.config = config
        self.ids = ids 
        self._zephir_db = ZephirDatabase(self.config.get("zephirdb_connect_str"))
        self._local_minter_db = None
        self._leveldb_primary_path = self.config.get("leveldb_primary_path")
        self._leveldb_cluster_path = self.config.get("leveldb_cluster_path")
     
    def mint_cid(self):
        ocns = self.ids.get("ocns")

        results = cid_inquiry_by_ocns(ocns, self._zephir_db, self._leveldb_primary_path, self._leveldb_cluster_path) 
        print(results)
        return results['min_cid']


