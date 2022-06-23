import os
import sys

import logging
from pathlib import PurePosixPath

from pymarc import MARCReader, MARCWriter, XMLWriter, TextWriter
from pymarc import marcxml
from pymarc import Record, Field

import xml.dom.minidom

from lib.utils import db_connect_url
from lib.utils import get_configs_by_filename
from cid_minter.cid_minter import CidMinter

def output_marc_records(config, input_file, output_file, err_file):
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
                ids = {
                    "ocns": [1,6567842,6758168,8727632],
                }
                cid = mint_cid(config, ids)
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

def mint_cid(config, ids):
    # call cid_minter to assinge a CID
    cid_minter = CidMinter(config, ids)
    cid = cid_minter.mint_cid()
    return cid

def convert_to_pretty_xml(input_file, output_file):
    dom = xml.dom.minidom.parse(input_file)
    pretty_xml_as_string = dom.toprettyxml()
    with open(output_file, 'w') as fh:
        fh.write(pretty_xml_as_string)



def main():
    env = sys.argv[1]
    if env not in ["test", "dev", "stg", "prd"]:
        usage(sys.argv[0])
        exit(1)

    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, 'config')

    zephir_db_config = get_configs_by_filename(CONFIG_PATH, "zephir_db")
    db_connect_str = str(db_connect_url(zephir_db_config[env]))

    cid_minting_config = get_configs_by_filename(CONFIG_PATH, "cid_minting")
    primary_db_path = cid_minting_config["primary_db_path"]
    cluster_db_path = cid_minting_config["cluster_db_path"]
    logfile = cid_minting_config['logpath']
    cid_inquiry_data_dir = cid_minting_config['cid_inquiry_data_dir']
    cid_inquiry_done_dir = cid_minting_config['cid_inquiry_done_dir']

    logging.basicConfig(
            level=logging.DEBUG,
            filename=logfile,
            format="%(asctime)s %(levelname)-4s %(message)s",
        )
    logging.info("Start " + os.path.basename(__file__))
    logging.info("Env: {}".format(env))

    DB_CONNECT_STR = os.environ.get("OVERRIDE_DB_CONNECT_STR") or db_connect_str
    PRIMARY_DB_PATH = os.environ.get("OVERRIDE_PRIMARY_DB_PATH") or primary_db_path
    CLUSTER_DB_PATH = os.environ.get("OVERRIDE_CLUSTER_DB_PATH") or cluster_db_path

    config = {
        "zephirdb_connect_str": DB_CONNECT_STR,
        "leveldb_primary_path": PRIMARY_DB_PATH,
        "leveldb_cluster_path": CLUSTER_DB_PATH,
    }

    input_file = f"{ROOT_PATH}/test_data/test_1_ia-coo-2_20220511.xml"
    output_file_tmp = f"{ROOT_PATH}/test_data/test_1_ia-coo-2_20220511_output_tmp.xml"
    output_file = f"{ROOT_PATH}/test_data/test_1_ia-coo-2_20220511_output.xml"
    err_file_tmp = f"{ROOT_PATH}/test_data/test_1_ia-coo-2_20220511_err_tmp.xml"
    err_file = f"{ROOT_PATH}/test_data/test_1_ia-coo-2_20220511_err.xml"

    print("Input file: ", input_file)
    print("Output file: ", output_file)
    print("Error file: ", err_file)

    output_marc_records(config, input_file, output_file_tmp, err_file_tmp)

    convert_to_pretty_xml(output_file_tmp, output_file)
    convert_to_pretty_xml(err_file_tmp, err_file)

    for file in [output_file_tmp, err_file_tmp]:
        if os.path.exists(file):
            os.remove(file)

    print("Finished")

if __name__ == '__main__':
    main()
