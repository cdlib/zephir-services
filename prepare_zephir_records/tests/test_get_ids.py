import os

import pytest
#from pymarc import MARCReader, MARCWriter, XMLWriter, TextWriter
from pymarc import marcxml
from pymarc import Record, Field

from read_write_zephir_records import get_ids

def test_get_ids_(data_dir):

    filename  = "test_record_nrlf-6.xml"
    record_file = os.path.join(data_dir, filename)

    record_1_ids = {
        "htid": "uc1.b3381876",
        "ocns": "7636130",
        "contribsys_ids": "nrlf.991064139089706532",
        "previous_contribsys_ids": "nrlf.b168484869"
    }
    record_2_ids = {
        "htid": "uc1.$b79686",
        "ocns": "2421602,24216021234",
        "contribsys_ids": "nrlf.991065875169706532",
        "previous_contribsys_ids": "nrlf.b120746153,nrlf.b1207461531234"
    }

    with open(record_file, 'rb') as fh:
        reader = marcxml.parse_xml_to_array(fh, strict=True, normalize_form=None)

        count = 0
        for record in reader:
            if record:
                count +=1
                record_no  = record.get_fields("001")[0]
                ids = get_ids(record)
                if count == 1: 
                    assert ids == record_1_ids
                elif count == 2:
                    assert ids == record_2_ids

