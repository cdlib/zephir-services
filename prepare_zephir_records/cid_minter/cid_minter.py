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
        htid =  None
        ocns = None
        sysids = None
        previous_sysids = None
        current_cid = None
        assigned_cid = None

        htid = self.ids.get("htid")
        ocns = convert_comma_separated_str_to_int_list(self.ids.get("ocns"))
        if self.ids.get("contribsys_ids"):
            sysids = self.ids.get("contribsys_ids").split(",")
        if self.ids.get("previous_sysids"):
            previous_sysids = self.ids.get("previous_sysids").split(",")

        if htid is None:
            logging.error("ID error: missing required htid")
            raise ValueError("ID error: missing required htid")

        logging.info(f"Find current CID by htid: {htid}")
        if self._zephir_db.find_cid_by_htid(htid):
            current_cid = self._zephir_db.find_cid_by_htid(htid)[0].get("cid")
            logging.info(f"Found current CID: {current_cid} by htid: {htid}")
        else:
            logging.info(f"No CID/item found in Zephir DB by htid: {htid}")

        if ocns:
            assigned_cid = self._find_cid_in_local_minter("ocn", ocns)
            if assigned_cid:
                if current_cid and current_cid != assigned_cid:
                    logging.info(f"htid {htid} changed CID from: {current_cid} to: {assigned_cid}")
                return assigned_cid

            assigned_cid = self._find_cid_in_zephir_by_ocns(ocns)
            if assigned_cid:
                if current_cid and current_cid != assigned_cid:
                    logging.info(f"htid {htid} changed CID from: {current_cid} to: {assigned_cid}")
                return assigned_cid
        else:
            logging.info(f"No OCLC number: Record {htid} does not contain OCLC number.")

        if sysids:
            assigned_cid = self._find_cid_in_local_minter("contribsys_id", sysids)
            if assigned_cid:
                if current_cid and current_cid != assigned_cid:
                    logging.info(f"htid {htid} changed CID from: {current_cid} to: {assigned_cid}")
                return assigned_cid

            assigned_cid = self._find_cid_in_zephir_by_sysids(sysids)

        return assigned_cid 

    def _find_cid_in_local_minter(self, id_type, values):
        logging.info(f"Find CID in local minter by {id_type}: {values}")
        assigned_cid = None
        matched_cids = []
        for value in values:
            results = self._local_minter_db.find_cid(id_type, str(value))
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
        """Search Zephir clusters by OCLC numbers.
           Return matched cluster ID.
           Sample search results:
           {
           'inquiry_ocns': [80274381, 25231018], 
           'matched_oclc_clusters': [[25231018], [80274381]], 
           'num_of_matched_oclc_clusters': 2, 
           'inquiry_ocns_zephir': [25231018, 80274381], 
           'cid_ocn_list': [{'cid': '009705704', 'ocn': '25231018'}, {'cid': '009705704', 'ocn': '80274381'}], 
           'cid_ocn_clusters': {'009705704': ['25231018', '80274381']}, 
           'num_of_matched_zephir_clusters': 1, 
           'min_cid': '009705704'
           }
        """
        logging.info(f"Find CID in Zephir Database by OCNs: {ocns}")
        assigned_cid = None
        results = cid_inquiry_by_ocns(ocns, self._zephir_db, self._leveldb_primary_path, self._leveldb_cluster_path)
        logging.info(f"Minting results from Zephir by OCNs: {results}")

        if results:
            num_of_matched_oclc_clusters = results.get('num_of_matched_oclc_clusters')
            num_of_matched_zephir_clusters = results.get('num_of_matched_zephir_clusters')
            assigned_cid = results.get('min_cid')
            cid_list = list(dict.fromkeys(results.get('cid_ocn_clusters')));

            if num_of_matched_oclc_clusters:
                if num_of_matched_oclc_clusters > 1:
                    logging.info(f"ZED code: pr0096 - Record OCNs {ocns} match more than one OCLC Concordance clusters")
            else:
                logging.error(f"ZED code: XXXX - OCLC Concordance Table does not contain record OCNs {ocns} ")

            if num_of_matched_zephir_clusters == 0:
                logging.info(f"Zephir minter: No CID found by OCNs: {ocns}")
            else:
                logging.info(f"Zephir minter: Found matched CID: {cid_list} by OCNs: {ocns}")

            if num_of_matched_zephir_clusters > 1:
                msg_detail = f"Record with OCLCs ({ocns}) matches {num_of_matched_zephir_clusters} CIDs ({cid_list}) used {assigned_cid}"
                if len(ocns) > 1:
                    logging.warning(f"ZED code: pr0090 - Record with OCLC numbers match more than one CID. - {msg_detail}")
                else:
                    logging.warning(f"ZED code: pr0091 - Record with one OCLC matches more than one CID. - {msg_detail}")

        return assigned_cid

    def _find_cid_in_zephir_by_sysids(self, sysids):
        """Search Zephir clusters by contrib sysids.
           Return matched cluster ID.
           Sample search results: 
             [{'cid': '002492721', 'contribsys_id': 'pur215476'}, {'cid': '009705704', 'contribsys_id': 'hvd000012735'}]
        """
        logging.info(f"Find CID in Zephir Database by contribsys IDs: {sysids}")
        assigned_cid = None
        results = self._zephir_db.find_zephir_clusters_by_contribsys_ids(sysids)
        logging.info(f"Minting results from Zephir by sysids: {results}")
        
        if results:
            assigned_cid = min(item.get('cid') for item in results )
            cid_list = list(item.get('cid') for item in results) 
            logging.info(f"Zephir minter: Found matched CIDs: {cid_list} by contrib sysids: {sysids}")
            if len(results) > 1:
                msg_detail = f"Record with local num ({sysids}) matches {len(results)} CIDs ({cid_list}) used {assigned_cid}";
                logging.warning(f"ZED code: pr0089 - Record with local number matches more than one CID. - {msg_detail} ")

            if self._cluster_contain_multiple_contribsys(assigned_cid):
                logging.warning(f"Zephir cluster contains records from different contrib systems. Skip this CID ({assigned_cid}) assignment")
                assigned_cid = None
        else:
            logging.info(f"Zephir minter: No CID found by local num: {sysids}")

        return assigned_cid

    def _cluster_contain_multiple_contribsys(self, cid):
        results = self._zephir_db.find_zephir_clusters_and_contribsys_ids_by_cid([cid])
        if results and len(results) > 1:
            return True
        else:
            return False
