import os
import sys

import argparse
import logging
from pathlib import PurePosixPath

from pymarc import MARCReader, MARCWriter, XMLWriter, TextWriter
from pymarc import marcxml
from pymarc import Record, Field

import xml.dom.minidom

from lib.utils import db_connect_url
from lib.utils import get_configs_by_filename
from cid_minter.cid_minter import CidMinter

def assign_cids(config, input_file, output_file, err_file):
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
                        logging.error("Error - more than one CID field. log error and skip this record")
                        writer_err.write(record)
                        continue
                    writer.write(record)
                else:
                    logging.error("Error - CID minting failed. log error and skip this record")
                    writer_err.write(record)
                    continue
            elif isinstance(reader.current_exception, exc.FatalReaderError):
                # data file format error
                # reader will raise StopIteration
                logging.error(f"Pymarc error: {reader.current_exception}")
                logging.error(f"Current chunk: {reader.current_chunk}")
            else:
                # fix the record data, skip or stop reading:
                logging.error(f"Pymarc error: {reader.current_exception}")
                logging.error(f"Current chunk: {reader.current_chunk}")
                # break/continue/raise
                writer_err.write(reader.current_chunk)
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

def config_logger(logfile, console):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    log_format = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s: %(message)s')
    # output to file
    file = logging.FileHandler(logfile)
    file.setFormatter(log_format)

    logger.addHandler(file)

    if console:   
        # output to console
        stream = logging.StreamHandler()
        logger.addHandler(stream)

def main():
    parser = argparse.ArgumentParser(description="Assign CID to Zephir records.")
    parser.add_argument("--console", "-c", action="store_true", dest="console", help="display log entries on screen")
    parser.add_argument("--env", "-e", nargs="?", dest="env", choices=["test", "dev", "stg", "prd"], required=True, help="define runtime environment")
    parser.add_argument("--source_dir", "-s", nargs="?", dest="source_dir", required=True, help="source file directory")
    parser.add_argument("--target_dir", "-t", nargs="?", dest="target_dir", required=True, help="target file directroy")
    parser.add_argument("--infile", "-i", nargs="?", dest="input_filename", required=True, help="input filename")
    parser.add_argument("--outfile", "-o", nargs="?", dest="output_filename", required=False, help="output filename")

    args = parser.parse_args()

    console = args.console
    env = args.env
    source_dir = args.source_dir
    target_dir = args.target_dir
    input_filename = args.input_filename
    output_filename = args.output_filename

    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, "config")

    zephirdb_config = get_configs_by_filename(CONFIG_PATH, "zephir_db")
    zephirdb_conn_str = str(db_connect_url(zephirdb_config[env]))
    local_minterdb_conn_str = zephirdb_conn_str

    cid_minting_config = get_configs_by_filename(CONFIG_PATH, "cid_minting")

    primary_db_path = cid_minting_config["primary_db_path"]
    cluster_db_path = cid_minting_config["cluster_db_path"]
    logfile = cid_minting_config["logpath"]

    config = {
        "zephirdb_conn_str": zephirdb_conn_str,
        "local_minterdb_conn_str": local_minterdb_conn_str,
        "leveldb_primary_path": primary_db_path,
        "leveldb_cluster_path": cluster_db_path,
    }

    config_logger(logfile, console)

    logging.info("Start " + os.path.basename(__file__))
    logging.info("Env: {}".format(env))

    if output_filename is None:
        output_filename = input_filename

    err_filename = f"{input_filename}.err"
    input_file = os.path.join(source_dir, input_filename)
    output_file = os.path.join(target_dir, output_filename)
    err_file = os.path.join(target_dir, err_filename)
    output_file_tmp = f"/tmp/{output_filename}.tmp"
    err_file_tmp = f"/tmp/{err_filename}.tmp"

    if input_file == output_file:
        err_msg = f"Filename error: Input and output files share the same path and name. ({input_file})"
        logging.error(err_msg)
        print("Exiting ...")
        print(err_msg)
        exit(1)

    print("For Testing: Input file: ", input_file)
    print("For Testing: Output file: ", output_file)
    print("For Testing: Error file: ", err_file)
    print("For Testing: tmp output: ",  output_file_tmp)
    print("For Testing: tmp error: ", err_file_tmp)

    assign_cids(config, input_file, output_file_tmp, err_file_tmp)

    convert_to_pretty_xml(output_file_tmp, output_file)
    convert_to_pretty_xml(err_file_tmp, err_file)

    #for file in [output_file_tmp, err_file_tmp]:
    #    if os.path.exists(file):
    #        os.remove(file)

    logging.info("Finished " + os.path.basename(__file__))
    print("For Testing: Finished " + os.path.basename(__file__))

if __name__ == '__main__':
    main()
