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
                #ids = {"ocns": "80274381,25231018", "contribsys_id": "hvd.000012735,hvd000012735", "previous_sysids": "", "htid": "hvd.hw5jdo"}
                ids = get_ids(record)
                cid = mint_cid(config, ids)
                if cid:
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
                else:
                    print("Error - CID minting failed. log error and skip this record")
                    writer_err.write(record)
                    continue
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

def get_ids(record):
    """Get IDs from the following fields:
      OCLC numbers (OCNs): 035$a fields with prefix (OCoLC)
      htid: HOL$p
      contribsys_ids: HOL$0
      previous_contribsys_ids: HOT$f 
    Format IDs as:
      multiple IDs: separate by a comma without any spaces
      contribsys_ids: remove prefix "sdr-"
      previous_contribsys_ids: Prefix with the campus code stored in CAT$c.
    Note:
      Contribsys ID and previous contribsys ID are in the <campus_code>.<local_number> format.
      Some early Zephir records may have contribsys IDs without the dot separator as: <campus_code><local_number>.
      Construct contrib and previous contrib sys IDs in both formats to ensure early records without the dot separator can be matched.
      Output the IDs in the sequence of: <campus_code>.<local_number>,<campus_code><local_number>
    Args: 
      record: MARC record
    Return: dictionary with key and value pairs for different IDs.
      keys:
        "htid",
        "ocns",
        "contribsys_ids"
        "previous_contribsys_ids"
      Values are strings. Multiple values for the same ID are separated by a comma without any spaces.
      Sample return:
      {
        "htid": "hvd.hw5jdo",
        "ocns": "80274381,25231018", 
        "contribsys_ids": "hvd.000012735,hvd000012735", 
        "previous_contribsys_ids": "hvd.000660168,hvd.000660168,hvd.O007B00250,hvdO007B00250", 
      }

    """
    ocns = None
    htid = None
    sysid = None
    prev_sysids = None
    campus_code = "" 
    contribsys_ids = None
    previous_contribsys_ids =  None

    for field in record.get_fields("035"):
        for sub_f in field.get_subfields('a'):
            if sub_f.startswith("(OCoLC)"): 
                ocn = sub_f.replace("(OCoLC)", "")
                ocns = f"{ocns},{ocn}" if ocns else ocn

    fields = record.get_fields("CAT")
    if fields and fields[0].get_subfields('c'):
        campus_code = fields[0].get_subfields('c')[0]

    fields = record.get_fields("HOL")
    if fields:
        if fields[0].get_subfields('p'):
            htid = fields[0].get_subfields('p')[0]
        if fields[0].get_subfields('0'):
            sysid = fields[0].get_subfields('0')[0]
        if fields[0].get_subfields('f'):
            prev_sysids = fields[0].get_subfields('f')[0]

    if sysid:
        sysid = sysid.replace("sdr-", "")
        if campus_code:
            sysid_2 = ""
            if sysid.startswith(f"{campus_code}."):
                sysid_2 = sysid.replace(f"{campus_code}.", campus_code)
                contribsys_ids = f"{sysid},{sysid_2}"
            else:
                sysid_2 = sysid.replace(campus_code, f"{campus_code}.")
                contribsys_ids = f"{sysid_2},{sysid}"
        else:
            contribsys_ids = sysid

    if prev_sysids:
        for p_id in prev_sysids.split(","):
            prefixed_id = f"{campus_code}.{p_id},{campus_code}{p_id}" if campus_code else p_id
            previous_contribsys_ids = f"{previous_contribsys_ids},{prefixed_id}" if previous_contribsys_ids else prefixed_id
            # add code to handle the dot separator

    ids = {}
    if htid:
        ids["htid"] = htid
    if ocns:
        ids["ocns"] = ocns
    if contribsys_ids:
        ids["contribsys_ids"] = contribsys_ids
    if previous_contribsys_ids:
        ids["previous_contribsys_ids"] = previous_contribsys_ids

    return ids


def mint_cid(config, ids):
    # call cid_minter to assinge a CID
    try:
        cid_minter = CidMinter(config)
        cid = cid_minter.mint_cid(ids)
        return cid
    except Exception as ex:
        return None


def convert_to_pretty_xml(input_file, output_file):
    dom = xml.dom.minidom.parse(input_file)
    pretty_xml_as_string = dom.toprettyxml()
    with open(output_file, 'w') as fh:
        fh.write(pretty_xml_as_string)

def config_logger(logfile):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    log_format = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s: %(message)s')
    # output to file
    file = logging.FileHandler(logfile)
    file.setFormatter(log_format)

    # output to console
    stream = logging.StreamHandler()

    logger.addHandler(file)
    logger.addHandler(stream)

def main():
    env = sys.argv[1]
    if env not in ["test", "dev", "stg", "prd"]:
        usage(sys.argv[0])
        exit(1)

    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, "config")

    zephirdb_config = get_configs_by_filename(CONFIG_PATH, "zephir_db")
    zephirdb_conn_str = str(db_connect_url(zephirdb_config[env]))

    localdb_config = get_configs_by_filename(CONFIG_PATH, "cid_minting")
    localdb_conn_str = str(db_connect_url(localdb_config[env]["minter_db"]))

    primary_db_path = localdb_config[env]["primary_db_path"]
    cluster_db_path = localdb_config[env]["cluster_db_path"]
    logfile = localdb_config["logpath"]

    ZEPHIRDB_CONN_STR = os.environ.get("OVERRIDE_ZEPHIRDB_CONN_STR") or zephirdb_conn_str
    LOCALDB_CONN_STR = os.environ.get("OVERRIDE_LOCALDB_CONN_STR") or localdb_conn_str
    PRIMARY_DB_PATH = os.environ.get("OVERRIDE_PRIMARY_DB_PATH") or primary_db_path
    CLUSTER_DB_PATH = os.environ.get("OVERRIDE_CLUSTER_DB_PATH") or cluster_db_path

    config = {
        "zephirdb_conn_str": ZEPHIRDB_CONN_STR,
        "localdb_conn_str": LOCALDB_CONN_STR,
        "leveldb_primary_path": PRIMARY_DB_PATH,
        "leveldb_cluster_path": CLUSTER_DB_PATH,
    }

    config_logger(logfile)

    logging.info("Start " + os.path.basename(__file__))
    logging.info("Env: {}".format(env))

    #input_file = f"{ROOT_PATH}/test_data/test_1_ia-coo-2_20220511.xml"
    #output_file_tmp = f"{ROOT_PATH}/test_data/test_1_ia-coo-2_20220511_output_tmp.xml"
    #output_file = f"{ROOT_PATH}/test_data/test_1_ia-coo-2_20220511_output.xml"
    #err_file_tmp = f"{ROOT_PATH}/test_data/test_1_ia-coo-2_20220511_err_tmp.xml"
    #err_file = f"{ROOT_PATH}/test_data/test_1_ia-coo-2_20220511_err.xml"

    # 20221027140504.nrlf-6.nrlf-6-nrlf-6_20221021.1.xml
    # test_record_nrlf-6_20221021.xml

    #filename = "20221027140504.nrlf-6.nrlf-6-nrlf-6_20221021.1.xml"
    filename = "test_record_nrlf-6_20221021.xml"
    filename_out = f"{filename}.cid"
    filename_err = f"{filename}.err"
    file_dir = "/apps/htmm/import/nrlf-6/cidfiles/"
    input_file = f"{file_dir}{filename}"
    output_file = f"{file_dir}{filename_out}"
    err_file = f"{file_dir}{filename_err}"
    output_file_tmp = f"/tmp/{filename_out}.tmp"
    err_file_tmp = f"/tmp/{filename_err}.tmp"


    print("Input file: ", input_file)
    print("Output file: ", output_file)
    print("Error file: ", err_file)
    print("tmp  out: ",  output_file_tmp)
    print("tmp error: ", err_file_tmp)

    output_marc_records(config, input_file, output_file_tmp, err_file_tmp)

    convert_to_pretty_xml(output_file_tmp, output_file)
    convert_to_pretty_xml(err_file_tmp, err_file)

    #for file in [output_file_tmp, err_file_tmp]:
    #    if os.path.exists(file):
    #        os.remove(file)

    print("Finished")

if __name__ == '__main__':
    main()
