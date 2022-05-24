import os

from pathlib import PurePosixPath

from pymarc import MARCReader, MARCWriter, XMLWriter, TextWriter
from pymarc import marcxml
from pymarc import Record, Field

import xml.dom.minidom

def output_marc_records(input_file, output_file, err_file):
    """Process input records and write records to output files based on specifications.

    Args:
    input_file:
    output_file:
    err_file:

    """

    writer = XMLWriter(open(output_file,'wb'))
    writer_err = XMLWriter(open(err_file,'wb'))
    with open(input_file, 'rb') as fh:
        reader = marcxml.parse_xml_to_array(fh, strict=True, normalize_form=None)
        """strict=True: check the namespaces for the MARCSlim namespace.
           Valid values for normalize_form are 'NFC', 'NFKC', 'NFD', and 'NFKD
           See unicodedata.normalize for more info on these
        """

        for record in reader:
            if record:
                cid = mint_cid()
                cid_fields = record.get_fields("CID")
                if not cid_fields:
                    record.add_field(Field(tag = 'CID', indicators = [' ',' '], subfields = ['a', cid]))
                elif len(cid_fields) == 1:
                    record["CID"]['a'] = cid 
                else:
                    print("Error - more than one CID field. log error and skip this record")
                    writer_err.write(record)
                    continue
                writer.write(record)
            elif isinstance(reader.current_exception, exc.FatalReaderError):
                # data file format error
                # reader will raise StopIteration
                print(reader.current_exception)
                print(reader.current_chunk)
            else:
                # fix the record data, skip or stop reading:
                print(reader.current_exception)
                print(reader.current_chunk)
                # break/continue/raise
                continue

    writer.close()
    writer_err.close()

def mint_cid():
    # call cid_minter to assinge a CID
    return "1000"

def convert_to_pretty_xml(input_file, output_file):
    dom = xml.dom.minidom.parse(input_file)
    pretty_xml_as_string = dom.toprettyxml()
    with open(output_file, 'w') as fh:
        fh.write(pretty_xml_as_string)



def main():
    input_file = "./test_data/test_1_ia-coo-2_20220511.xml"
    output_file_tmp = "./test_data/test_1_ia-coo-2_20220511_output_tmp.xml"
    output_file = "./test_data/test_1_ia-coo-2_20220511_output.xml"
    err_file_tmp = "./test_data/test_1_ia-coo-2_20220511_err_tmp.xml"
    err_file = "./test_data/test_1_ia-coo-2_20220511_err.xml"

    print("Input file: ", input_file)
    print("Output file: ", output_file)
    print("Error file: ", err_file)

    output_marc_records(input_file, output_file_tmp, err_file_tmp)

    convert_to_pretty_xml(output_file_tmp, output_file)
    convert_to_pretty_xml(err_file_tmp, err_file)

    for file in [output_file_tmp, err_file_tmp]:
        if os.path.exists(file):
            os.remove(file)

    print("Finished")

if __name__ == '__main__':
    main()
