import json

import pymarc


class VufindFormatter:
    """VufindFormatter is a utility class for creating vufind records.

    To use this class, use the create_record method. See documentation below.

    """

    @staticmethod
    def create_record(cid, records):
        """The create_record method produces a formatted VuFind records from a
        cluster identifier and a list of unparsed-json records.

        Note: The first record in the list becomes the "base record" for the
        group.

        Args:
            cid: The CID for the created record, to be used in the 001.
            records: A list of strings, representing the json records to be
            parsed and combined into a single record.

        Returns:
            True if successful, False otherwise.

        Raises:
            ValueError: Raised if the record list is empty.

            """

        base_sdr = None
        base_record = None
        holdings = []
        sdrs = []
        incl_sdrs = set()

        if len(records) == 0:
            raise ValueError("Record list may not be empty")

        for record in records:
            for marc_record in pymarc.JSONReader(record):

                # transfer source and collection code from bib-level field to 974 item-level field
                VufindFormatter._insert_subfield("c", marc_record["HOL"]["c"], marc_record["974"].subfields)
                VufindFormatter._insert_subfield("b", marc_record["HOL"]["s"], marc_record["974"].subfields)

                # todo(cscollett): clean up after reconciling period is done
                if cid != marc_record["CID"]["a"]:
                    print("{} mismatched with {}".format(cid, marc_record["CID"]["a"]))

                if base_record is None:
                    # prepare the base record (first record by convention)
                    base_record = marc_record
                    base_record["001"].data = cid
                    for field in marc_record.get_fields("035"):
                        if field.value().startswith("sdr-"):
                            incl_sdrs.add(field.value())
                            base_sdr = field
                else:
                    # prepare 947 holdings, and holding 035 sdr
                    holdings.append(marc_record["974"])
                    for field in marc_record.get_fields("035"):
                        if field.value().startswith("sdr-"):
                            sdrs.append(field)

        for holding in holdings:
            base_record.add_field(holding)

        for sdr_field in sdrs:
            # only include new sdrs (different holdings can share sdrs)
            if sdr_field.value() not in incl_sdrs:
                # insert sdr below last sdr
                base_sdr_idx = base_record.fields.index(base_sdr)
                idx_offset = len(incl_sdrs)
                base_record.fields.insert(base_sdr_idx + idx_offset, sdr_field)
                incl_sdrs.add(sdr_field.value())

        return base_record

    @staticmethod
    def _insert_subfield(indicator, value, subfields_list):
        """The _insert_subfield method is a helper to insert a new subfield in
        alpha-numeric order.

        """

        pos = 0
        for idx, subfield_item in enumerate(subfields_list):
            if idx % 2 == 0:
                if indicator <= subfield_item:
                    pos = idx
                    break
                if indicator > subfield_item:
                    pos = idx + 2

        subfields_list.insert(pos, value)
        subfields_list.insert(pos, indicator)
