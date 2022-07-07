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

        assigned_cid = self._find_cid_in_local_minter("ocn", ocns)
        if assigned_cid:
            if assigned_cid != current_cid:
                logging.info(f"htid {htid} changed CID from: {current_cid} to: {assigned_cid}")
            return assigned_cid

        assigned_cid = self._find_cid_in_zephir_by_ocns(ocns)
        if assigned_cid:
            if assigned_cid != current_cid:
                logging.info(f"htid {htid} changed CID from: {current_cid} to: {assigned_cid}")
            return assigned_cid

        logging.info(f"Find CID in Zephir Database by contribsys IDs: {sysid}")
        results = self._zephir_db.find_zephir_clusters_by_contribsys_ids([sysid])

        logging.info("Minting results by sysid:")
        logging.info(results)

        return assigned_cid 

    def _find_cid_in_local_minter(self, id_type, values):
        logging.info(f"Find CID in local minter by {id_type}: {values}")
        assigned_cid = None
        matched_cids = []
        for value in values:
            results = self._local_minter_db.find_cid(id_type, str(value))
            logging.info(f"Minting results from local minter by {id_type}: {value}: {results}")
            if results and results.get('matched_cid') not in matched_cids:
                matched_cids.append(results.get('matched_cid'))
        if len(matched_cids) == 0:
            logging.info(f"Local minter: No CID found by {id_type}: {values}")
        elif len(matched_cids) == 1:
            assigned_cid = matched_cids[0]
            logging.info(f"Local minter: Found matched CID: {matched_cids} by {id_type}: {values}")
        else:
            logging.error(f"Local minter error: Found more than one matched CID: {matched_cids} by {id_type}: {values}")

        return assigned_cid

    def _find_cid_in_zephir_by_ocns(self, ocns):
        logging.info(f"Find CID in Zephir Database by OCNs: {ocns}")
        assigned_cid = None
        results = cid_inquiry_by_ocns(ocns, self._zephir_db, self._leveldb_primary_path, self._leveldb_cluster_path)
        logging.info(f"Minting results by OCNs: {results}")

        if results:
            num_of_matched_oclc_clusters = results.get('num_of_matched_oclc_clusters')
            num_of_matched_zephir_clusters = results.get('num_of_matched_zephir_clusters')
            assigned_cid = results.get('min_cid')
            cid_list = list(dict.fromkeys(results.get('cid_ocn_clusters')));

            if num_of_matched_oclc_clusters > 1:
                logging.info("ZED code: pr0096 - Record OCNs match more than one OCLC Concordance clusters")

            if num_of_matched_zephir_clusters == 0:
                logging.info(f"Zephir minter: No CID found by OCNs: {ocns}")
            else:
                logging.info("Zephir minter: Found matched CID: {matched_cids} by OCNs: {ocns}")

            if num_of_matched_zephir_clusters > 1:
                msg_detail = f"Record with OCLCs ({ocns}) matches {num_of_matched_zephir_clusters} CIDs ({cid_list}) used {assigned_cid}"
                if len(ocns) > 1:
                    logging.warn(f"ZED code: pr0090 - Record with OCLC numbers match more than one CID. {msg_detail}")
                else:
                    logging.warn(f"ZED code: pr0091 - Record with one OCLC matches more than one CID. {msg_detail}")

        return assigned_cid
