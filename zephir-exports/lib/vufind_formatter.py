import json

import pymarc

class VufindFormatter:
    @staticmethod
    def create_record(cid, records):
        base_sdr = None
        base_record = None
        holdings = []
        sdrs = []
        incl_sdrs = set()

        for record in records:
            for marc_record in pymarc.JSONReader(record):
                marc_record['974'].subfields.insert(0, marc_record['HOL']['c'])
                marc_record['974'].subfields.insert(0, "c")
                marc_record['974'].subfields.insert(0, marc_record['HOL']['s'])
                marc_record['974'].subfields.insert(0, "b")
                if base_record is None:
                    base_record = marc_record
                    base_record["001"].data = cid
                    for field in marc_record.get_fields("035"):
                        if field.value().startswith('sdr-'):
                            incl_sdrs.add(field.value())
                            base_sdr = field
                else:
                    holdings.append(marc_record["974"])
                    for field in marc_record.get_fields("035"):
                        if field.value().startswith('sdr-'):
                            sdrs.append(field)

        if base_record is None or base_sdr is None:
            raise Exception("No base record available for formatting")

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
