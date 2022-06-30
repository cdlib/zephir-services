import os
import sys

import logging

from cid_minter.oclc_lookup import lookup_ocns_from_oclc
from cid_minter.zephir_cluster_lookup import ZephirDatabase
from cid_minter.cid_inquiry_by_ocns import cid_inquiry_by_ocns
from cid_minter.local_cid_minter import LocalMinter
from cid_minter.cid_inquiry_by_ocns import convert_comma_separated_str_to_int_list

class CidMinter:
    def __init__(self, config, ids):
        self.config = config
        self.ids = ids 
        self._zephir_db = ZephirDatabase(self.config.get("zephirdb_conn_str"))
        self._local_minter_db = LocalMinter(self.config.get("localdb_conn_str"))
        self._leveldb_primary_path = self.config.get("leveldb_primary_path")
        self._leveldb_cluster_path = self.config.get("leveldb_cluster_path")
     
    def mint_cid(self):
        htid = self.ids.get("htid")
        ocns = convert_comma_separated_str_to_int_list(self.ids.get("ocns"))
        sysid = self.ids.get("contribsys_id")
        previous_sysids = self.ids.get("previous_sysid")

        current_cid = None
        assigned_cid = None

        current_cid = self._zephir_db.find_cid_by_htid(htid)
        logging.info(f"current cid {current_cid}")

        results = cid_inquiry_by_ocns(ocns, self._zephir_db, self._leveldb_primary_path, self._leveldb_cluster_path) 
        logging.info("minting results by ocns:")
        logging.info(results)

        assigned_cid = results['min_cid']

        results = self._zephir_db.find_zephir_clusters_by_contribsys_ids([sysid])
        logging.info("minting results by sysid")
        logging.info(results)

        return assigned_cid 


