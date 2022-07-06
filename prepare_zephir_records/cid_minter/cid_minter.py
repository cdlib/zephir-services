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
        """Assign CID by OCNs, local system IDs or previous local system IDs.
        Return assgined CID.
        """
        htid = self.ids.get("htid")
        ocns = convert_comma_separated_str_to_int_list(self.ids.get("ocns"))
        sysid = self.ids.get("contribsys_id")
        previous_sysids = self.ids.get("previous_sysid")

        current_cid = None
        assigned_cid = None

        logging.info(f"Find current CID by htid: {htid}")
        current_cid = self._zephir_db.find_cid_by_htid(htid)
        if current_cid:
            logging.info(f"Found current CID: {current_cid} by htid: {htid}")
        else:
            logging.info(f"No CID/item found in Zephir DB by htid: {htid}")

        logging.info(f"Find CID in local minter by OCNs: {ocns}")
        matched_cids = []
        for ocn in ocns:
            results = self._local_minter_db.find_cid("ocn", str(ocn))
            logging.info(f"Minting results from local minter by OCN: {ocn}: {results}")
            if results and results.get('matched_cid') not in matched_cids:
                matched_cids.append(results.get('matched_cid'))
        if len(matched_cids) == 0:
            logging.info(f"Local minter: No CID found by OCNs: {ocns}")
        elif len(matched_cids) == 1:
            assigned_cid = matched_cids[0]
            logging.info(f"Local minter: Found matched CID: {matched_cids} by OCNs: {ocns}")
        else:
            logging.error(f"Local minter error: Found more than one matched CID: {matched_cids} by OCNs: {ocns}")
            logging.info(results)

        if assigned_cid:
            return assigned_cid

        logging.info(f"Find CID in Zephir Database by OCNs: {ocns}")
        results = cid_inquiry_by_ocns(ocns, self._zephir_db, self._leveldb_primary_path, self._leveldb_cluster_path) 
        logging.info(f"Minting results by OCNs: {results}")

        assigned_cid = results['min_cid']

        logging.info(f"Find CID in Zephir Database by contribsys IDs: {sysid}")
        results = self._zephir_db.find_zephir_clusters_by_contribsys_ids([sysid])

        logging.info("Minting results by sysid:")
        logging.info(results)

        return assigned_cid 


