import os
import sys
from enum import Enum

import logging

from cid_minter.oclc_lookup import lookup_ocns_from_oclc
from cid_minter.zephir_cluster_lookup import ZephirDatabase
from cid_minter.zephir_cluster_lookup import CidMinterTable 
from cid_minter.cid_inquiry_by_ocns import cid_inquiry_by_ocns
from cid_minter.cid_store import CidStore
from cid_minter.cid_inquiry_by_ocns import convert_comma_separated_str_to_int_list
from cid_minter.zed_for_cid import CidZedEvent

class IdType(Enum):
    OCN = "ocn"
    SYSID = "contribsys id" 
    PREV_SYSID = "previous contribsys id"


class CidMinter:
    """Mint CID from local minter and Zephir database based on given record IDs.
       Note: We have migrated the "local minter" from SQLite to MySQL based Zephir database. 
             The term "local minter" in this document is equivalent to the cid_minting_store table in the Zephir DB.
    """
    def __init__(self, config):
        self.config = config
        self._zephir_db = ZephirDatabase(self.config.get("zephirdb_conn_str"))
        self._minter_db = CidStore(self.config.get("minterdb_conn_str"))
        self._leveldb_primary_path = self.config.get("leveldb_primary_path")
        self._leveldb_cluster_path = self.config.get("leveldb_cluster_path")
        self.cid_zed_event = CidZedEvent(self.config.get("zed_msg_table"), self.config.get("zed_log"))
     
    def close(self):
        self.cid_zed_event.close()

    def mint_cid(self, ids):
        """Assign CID by OCNs, local system IDs or previous local system IDs.
        Search CID in the local minter first. If there is no matched CID found then search the Zephir database.
        Args:
          ids: IDs in a dictionary with following keys: 
                 "htid",
                 "ocns",
                 "contribsys_ids"
                 "previous_contribsys_ids"
               Values are strings. Multiple values for the same ID are separated by a comma.
        Returns: assgined CID.
        """
        htid =  None
        ocns = None
        sysids = None
        previous_sysids = None
        current_cid = None
        assigned_cid = None
        cid_assigned_by = None

        htid = ids.get("htid")
        ocns = convert_comma_separated_str_to_int_list(ids.get("ocns"))
        if ids.get("contribsys_ids"):
            sysids = ids.get("contribsys_ids").split(",")
        if ids.get("previous_contribsys_ids"):
            previous_sysids = ids.get("previous_contribsys_ids").split(",")

        if not htid:
            logging.error("ID error: missing required htid")
            raise ValueError("ID error: missing required htid")

        logging.info(f"Find current CID by htid: {htid}")
        results = self._zephir_db.find_cid_by_htid(htid)
        if results:
            current_cid = results[0].get("cid")
            logging.info(f"Found current CID: {current_cid} by htid: {htid}")
        else:
            logging.info(f"No CID/item found in Zephir DB by htid: {htid}")

        if ocns:
            assigned_cid = self._find_cid_in_local_minter(IdType.OCN, ocns)
            if not assigned_cid:
                assigned_cid = self._find_cid_in_zephir_by_ocns(ocns)
            if assigned_cid:
                cid_assigned_by = "OCLC number(s)"
        else:
            logging.info(f"No OCLC number: Record {htid} does not contain OCLC number.")

        if sysids and self._cid_not_assigned_yet(assigned_cid):
            assigned_cid = self._find_cid_in_local_minter(IdType.SYSID, sysids)
            if not assigned_cid:
                assigned_cid = self._find_cid_in_zephir_by_sysids(IdType.SYSID, sysids)
            if assigned_cid:
                cid_assigned_by = "campus local number"

        if previous_sysids and self._cid_not_assigned_yet(assigned_cid): 
            assigned_cid = self._find_cid_in_local_minter(IdType.PREV_SYSID, previous_sysids)
            if not assigned_cid:
                assigned_cid = self._find_cid_in_zephir_by_sysids(IdType.PREV_SYSID, previous_sysids)
            if assigned_cid:
                cid_assigned_by = "campus previous local number"

        if self._cid_assigned(assigned_cid) and current_cid and current_cid != assigned_cid:
            msg_code = "pr0059"
            msg_detail = f"WARNING: Hathi-id ({htid}) changed CID from: {current_cid} to: {assigned_cid}"
            logging.info(f"ZED code: {msg_code} - {msg_detail}")
            event_data = {
                "msg_detail": msg_detail,
            }
            self.cid_zed_event.merge_zed_event_data(event_data)
            self.cid_zed_event.create_zed_event(msg_code)

        if self._cid_not_assigned_yet(assigned_cid):
            current_minter = self._find_current_minter()
            self._minter_new_cid()
            assigned_cid = self._find_current_minter().get("cid")
            msg_code = "pr0212" # assign new CID
            logging.info(f"ZED code: {msg_code} - Minted a new minter: {assigned_cid} - from current minter: {current_minter}")
            event_data = {
                "msg_detail": f"Assigned new CID: {assigned_cid}",
                "report": {"CID": assigned_cid}
            }
            self.cid_zed_event.merge_zed_event_data(event_data)
            self.cid_zed_event.create_zed_event(msg_code)
        else:
            msg_code = "pr0213" # assigned existing CID 
            msg_detail = f"Assigned existing CID: {assigned_cid} - assigned by matching {cid_assigned_by}"
            logging.info(f"ZED code: {msg_code} - {msg_detail}")
            event_data = {
                "msg_detail": msg_detail,
                "report": {"CID": assigned_cid}
            }
            self.cid_zed_event.merge_zed_event_data(event_data)
            self.cid_zed_event.create_zed_event(msg_code)

        if assigned_cid:
            self._update_local_minter(ids, assigned_cid)

        return assigned_cid 

    def _cid_assigned(self, assigned_cid):
        if assigned_cid and assigned_cid != '0':
            return True
        else:
            return False

    def _cid_not_assigned_yet(self, assigned_cid):
        return not self._cid_assigned(assigned_cid)

    def _find_cid_in_local_minter(self, input_id_type, values):
        """Find CID in the local minter database.
        Args:
           input_id_type: ID type defined by enum class IDType.
           values: list of strings
        Returns: matched CID in string
        """
        if type(input_id_type) != IdType:
            err_msg = f"Data type error: ID type should be IdType. Type {type(input_id_type)} was used instead."
            logging.error(err_msg)
            raise TypeError(err_msg)

        logging.info(f"Find CID in local minter by {input_id_type.name}: {values}")

        if input_id_type == IdType.OCN:
            id_type = "ocn"
        else:
            id_type = "sysid"

        assigned_cid = None
        matched_cids = []

        for value in values:
            results = self._minter_db.find_cid(id_type, str(value))
            if results and results.get('matched_cid') not in matched_cids:
                matched_cids.append(results.get('matched_cid'))
        if len(matched_cids) == 0:
            logging.info(f"Local minter: No CID found by {input_id_type.name}: {values}")
        elif len(matched_cids) == 1:
            assigned_cid = matched_cids[0]
            logging.info(f"Local minter: Found matched CID: {matched_cids} by {input_id_type.name}: {values}")
        else:
            logging.error(f"Local minter error: Found more than one matched CID: {matched_cids} by {input_id_type.name}: {values}")

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
                    msg_code = "pr0096"
                    msg_detail = f"Record OCNs {ocns} match more than one OCLC Concordance clusters"
                    logging.info(f"ZED code: {msg_code} - {msg_detail}")
                    event_data = {
                        "msg_detail": msg_detail,
                    }
                    self.cid_zed_event.merge_zed_event_data(event_data)
                    self.cid_zed_event.create_zed_event(msg_code)
            else:
                msg_code = "pr0094"
                msg_detail = f"OCLC Concordance Table does not contain record OCNs {ocns}"
                logging.error(f"ZED code: {msg_code} - {msg_detail}")
                event_data = {
                    "msg_detail": msg_detail,
                }
                self.cid_zed_event.merge_zed_event_data(event_data)
                self.cid_zed_event.create_zed_event(msg_code)

            if num_of_matched_zephir_clusters == 0:
                logging.info(f"Zephir minter: No CID found by OCNs: {ocns}")
            else:
                logging.info(f"Zephir minter: Found matched CID: {cid_list} by OCNs: {ocns}")

            if num_of_matched_zephir_clusters > 1:
                plural = ""
                msg_code = "pr0091"
                if len(ocns) > 1:
                    msg_code = "pr0090"
                    plural = "s"
                msg_detail = f"Record with OCLC{plural} ({ocns}) matches {num_of_matched_zephir_clusters} CIDs ({cid_list}) used {assigned_cid}"
                logging.warning(f"ZED code: {msg_code} - {msg_detail}")
                event_data = {
                        "msg_detail": msg_detail,
                    }
                self.cid_zed_event.merge_zed_event_data(event_data)
                self.cid_zed_event.create_zed_event(msg_code)

        return assigned_cid

    def _find_cid_in_zephir_by_sysids(self, input_id_type, sysids):
        """Search Zephir clusters by contrib sysids.
        Args:
           input_id_type: ID type defined by enum class IDType.
           sysids: list of strings
        Returns: matched cluster ID.
           Sample search results: 
             [{'cid': '002492721', 'contribsys_id': 'pur215476'}, 
              {'cid': '009705704', 'contribsys_id': 'hvd000012735'}]
        """
        if type(input_id_type) != IdType:
            err_msg = f"Data type error: ID type should be IdType. Type {type(input_id_type)} was used instead."
            logging.error(err_msg)
            raise TypeError(err_msg)

        logging.info(f"Find CID in Zephir Database by {input_id_type.name}: {sysids}")

        if input_id_type not in [IdType.SYSID, IdType.PREV_SYSID]:
            err_msg = f"ID type error: ID type should be IdType.SYSID or IdType.PREV_SYSID. {input_id_type.name} was used instead."
            logging.error(err_msg)
            raise TypeError(err_msg)

        results = self._zephir_db.find_zephir_clusters_by_contribsys_ids(sysids)
        logging.info(f"Minting results from Zephir by {input_id_type.name}: {results}")
        
        if input_id_type == IdType.SYSID:
            return self._sysid_results(results, sysids)
        else:
            return self._previous_sysid_results(results, sysids)
        
    def _sysid_results(self, results, sysids):
        assigned_cid = None
        if results:
            assigned_cid = min(item.get('cid') for item in results )
            cid_list = list(item.get('cid') for item in results) 
            logging.info(f"Zephir minter: Found matched CIDs: {cid_list} by contribsys IDs: {sysids}")
            if len(results) > 1:
                msg_code = "pr0089"
                msg_detail = f"Record with local num ({sysids}) matches {len(results)} CIDs ({cid_list}) used {assigned_cid}";
                logging.warning(f"ZED code: {msg_code} - Record with local number matches more than one CID. - {msg_detail} ")
                event_data = {
                    "msg_detail": msg_detail,
                }
                self.cid_zed_event.merge_zed_event_data(event_data)
                self.cid_zed_event.create_zed_event(msg_code)

            if self._cluster_contain_multiple_contribsys(assigned_cid):
                logging.warning(f"Zephir cluster contains records from different contrib systems. Skip this CID ({assigned_cid}) assignment")
                assigned_cid = None
        else:
            logging.info(f"Zephir minter: No CID found by local num: {sysids}")

        return assigned_cid

    def _previous_sysid_results(self, results, sysids):
        assigned_cid = None
        if results:
            cid_list = list(item.get('cid') for item in results)
            logging.info(f"Zephir minter: Found matched CIDs: {cid_list} by previous contribsys IDs: {sysids}")
            if len(results) > 1:
                msg_code = "pr0042"
                msg_detail = f"Record with previous local num ({sysids}) matches {len(results)} CIDs ({cid_list})";
                zed_msg = f"ZED code: {msg_code} - Record with previous local num matches more than one CID. - {msg_detail}"
                logging.error(zed_msg)
                event_data = {
                    "msg_detail": msg_detail,
                }
                self.cid_zed_event.merge_zed_event_data(event_data)
                self.cid_zed_event.create_zed_event(msg_code)
                raise ValueError(zed_msg)
            else:
                assigned_cid = results[0].get('cid')
                if self._cluster_contain_multiple_contribsys(assigned_cid):
                    logging.warning(f"Zephir cluster contains records from different contrib systems. Skip this CID ({assigned_cid}) assignment")
                    assigned_cid = None
        else:
            logging.info(f"Zephir minter: No CID found by previous contribsys IDs: {sysids}")

        return assigned_cid

    def _cluster_contain_multiple_contribsys(self, cid):
        results = self._zephir_db.find_zephir_clusters_and_contribsys_ids_by_cid([cid])
        if results and len(results) > 1:
            return True
        else:
            return False

    def _minter_new_cid(self):
        cid_minter_table = CidMinterTable(self._zephir_db)
        return cid_minter_table.mint_a_new_cid()

    def _find_current_minter(self):
        cid_minter_table = CidMinterTable(self._zephir_db)
        return cid_minter_table.get_cid()

    def _update_local_minter(self, ids, cid):
        ocns = ids.get("ocns")
        sysids = ids.get("contribsys_ids")
        previous_sysids = ids.get("previous_contribsys_ids")
        if ocns:
            for ocn in ocns.split(","):
                self._minter_db.write_identifier("ocn", ocn, cid)
                logging.info(f"Updated local minter: ocn: {ocn}")

        if sysids:
            for sysid in sysids.split(","):
                self._minter_db.write_identifier("sysid", sysid, cid)
                logging.info(f"Updated local minter: contribsys id: {sysid}")

        if previous_sysids:
            for sysid in previous_sysids.split(","):
                self._minter_db.write_identifier("sysid", sysid, cid)
                logging.info(f"Updated local minter: previous contribsys id: {sysid}")

