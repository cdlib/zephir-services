import os

import pytest
#from pymarc import MARCReader, MARCWriter, XMLWriter, TextWriter
from pymarc import marcxml
from pymarc import Record, Field

from read_write_zephir_records import get_ids

def test_get_ids_(data_dir):

    filename  = "test_records_nrlf-6_20221021.xml"
    record_file = os.path.join(data_dir, filename)

    expected_ids = []
    # 0: single value
    expected_ids.append({
        "htid": "uc1.b3381876",
        "ocns": "7636130",
        "contribsys_ids": "nrlf.991064139089706532,nrlf991064139089706532",
        "previous_contribsys_ids": "nrlf.b168484869,nrlfb168484869"
    })
    # 1: multiple values separated by comma
    expected_ids.append({
        "htid": "uc1.$b79686",
        "ocns": "2421602,24216021234",
        "contribsys_ids": "nrlf.991065875169706532,nrlf991065875169706532",
        "previous_contribsys_ids": "nrlf.b120746153,nrlfb120746153,nrlf.b1207461531234,nrlfb1207461531234"
    })
    # 2: no ocn
    expected_ids.append({
        "htid": "uc1.b4372349",
        "contribsys_ids": "nrlf.991069357439706532,nrlf991069357439706532",
        "previous_contribsys_ids": "nrlf.b121928342,nrlfb121928342"
    })
    # 3: no ids - no subfields
    expected_ids.append({})
    # 4: no ids - no HOL field 
    expected_ids.append({})
    # 5: no CAT$a campus_code
    expected_ids.append({
        "htid": "uc1.b3073244",
        "ocns": "213746212",
        "contribsys_ids": "nrlf.991063883959706532",
        "previous_contribsys_ids": "b120066464"
    })

    with open(record_file, 'rb') as fh:
        reader = marcxml.parse_xml_to_array(fh, strict=True, normalize_form=None)

        idx = 0
        for record in reader:
            if record:
                record_no  = record.get_fields("001")[0]
                ids = get_ids(record)
                if idx <= 5:
                    assert ids ==  expected_ids[idx]
                else:
                    break
                idx +=1

