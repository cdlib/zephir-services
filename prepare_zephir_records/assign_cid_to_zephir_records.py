import os
import sys
from glob import glob

import argparse
import logging
from pathlib import PurePosixPath
from datetime import datatime

from pymarc import MARCReader, MARCWriter, XMLWriter, TextWriter
from pymarc import marcxml
from pymarc import Record, Field

from lxml import etree

from lib.utils import db_connect_url
from lib.utils import get_configs_by_filename
from cid_minter.cid_minter import CidMinter

def assign_cids(cid_minter, input_file, output_file, err_file):
    """Process input records and write records to output files based on specifications.

    Args:
      cid_minter: CidMinter object
      input_file: full path of input file
      output_file: full path of output file
      err_file: full path or error file
    """

    writer = XMLWriter(open(output_file,'wb'))
    writer_err = XMLWriter(open(err_file,'wb'))
    no_error = True
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
                cid = mint_cid(cid_minter, ids)
                if cid:
                    cid_fields = record.get_fields("CID")
                    if not cid_fields:
                        record.add_field(Field(tag = 'CID', indicators = [' ',' '], subfields = ['a', cid]))
                    elif len(cid_fields) == 1:
                        record["CID"]['a'] = cid 
                    else:
                        no_error = False
                        logging.error("Error - more than one CID field. log error and skip this record")
                        writer_err.write(record)
                        continue
                    writer.write(record)
                else:
                    no_error = False
                    logging.error("Error - CID minting failed. log error and skip this record")
                    writer_err.write(record)
                    continue
            elif isinstance(reader.current_exception, exc.FatalReaderError):
                # data file format error
                # reader will raise StopIteration
                logging.error(f"Pymarc error: {reader.current_exception}")
                logging.error(f"Current chunk: {reader.current_chunk}")
            else:
                no_error = False
                # fix the record data, skip or stop reading:
                logging.error(f"Pymarc error: {reader.current_exception}")
                logging.error(f"Current chunk: {reader.current_chunk}")
                # break/continue/raise
                writer_err.write(reader.current_chunk)
                continue

    writer.close()
    writer_err.close()
    if no_error:
        os.remove(err_file)

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


def mint_cid(cid_minter, ids):
    # call cid_minter to assinge a CID
    try:
        cid = cid_minter.mint_cid(ids)
        return cid
    except Exception as ex:
        return None


def convert_to_pretty_xml(input_file, output_file):
    try:
        parser = etree.XMLParser(remove_blank_text=True, recover=True)
        tree = etree.parse(input_file, parser)
        tree.write(output_file, pretty_print=True)
    except Exception as ex:
        err_msg = f"Pretty XMLParser error: {ex}"
        logging.error(err_msg)
        print(err_msg)

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

def process_one_file(config, source_dir, target_dir, input_filename, output_filename):
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
        return

    cid_minter = CidMinter(config)
    assign_cids(cid_minter, input_file, output_file_tmp, err_file_tmp)

    if os.path.exists(output_file_tmp):
        convert_to_pretty_xml(output_file_tmp, output_file)
    if os.path.exists(err_file_tmp): 
        convert_to_pretty_xml(err_file_tmp, err_file)

def lock_a_dir_for_cid_minting(dir_path):
    """Locate a directory for CID minting.
       Identify a dirctory that contains at least one Zephir prepared file with .xml extension but does not contain a process.cid file;
       Lock the identified directory for CID minting by putting a process.cid file underneath.
    Args:
      dir_path: a string for a directroy with or without wildcard char "*". For example:
        <htmm_home_dir>/import/miu-multi-2/prepared_files/
        <htmm_home_dir>/import/*/prepared_files/
    Returns:
      dirname_locked: the identified/locked directory
    """
    for a_dir in glob(dir_path):
        process_dot_cid = os.path.join(a_dir, "process.cid")
        if os.path.exists(process_dot_cid):
            print(f"Another CID minter is processing files in directroy {a_dir} - pass this dir")
            continue
        else:
            xml_files = os.path.join(a_dir, "*.xml")
            for file in glob(xml_files):
                print(f"Found an xml file to process: {file}")
                print("Lock this directory")
                with open(process_dot_cid, 'w') as fp:
                    pass
                return os.path.dirname(file)
    return None

def lock_a_file_for_cid_minting(a_dir, pid):
    """Locate an .xml file in the specified directory for CID minting.
       Find an .xml file in the identified directory;
       Lock the identified file by renaming it to filename.pid.
    Args:
      a_dir: a directory that contain Zephir prepared files
      pid: current PID
    Returns: 
      dirname_locked: the identified directory <htmm_home_dir>/import/<zephir_config_name>/prepared_files/ 
      filename_org: the name of the identified file
      filename_locked: the new filename with a .PID attached 
    """
    filename_org = None
    filename_locked = None

    xml_files = os.path.join(a_dir, "*.xml")
    for file in glob(xml_files):
        print(f"Found an xml file to process: {file}")
        print("Lock this file for CID minting")
        dirname_locked, filename_org = os.path.split(file)
        filename_locked = f"{filename_org}.{pid}"
        os.rename(file, os.path.join(dirname_locked, filename_locked))
        return filename_org, filename_locked 
    return filename_org, filename_locked 

def batch_process(config, dir_path, pid):
    """Batch process files in an identified directroy
    Args:
      config: a Python directroy for configurations 
      dir_path: a string for a directroy with or without wildcard char "*". For example:
        <htmm_home_dir>/import/miu-multi-2/prepared_files/
        <htmm_home_dir>/import/*/prepared_files/
      pid: PID of current process
    """
    locked_dir = lock_a_dir_for_cid_minting(dir_path)
    if locked_dir:
        parent_dir = os.path.dirname(locked_dir)
        target_dir = os.path.join(parent_dir, "cidfiles")
        # process all files in the locked directory 
        while True:
            filename_org, filename_locked = lock_a_file_for_cid_minting(locked_dir, pid)
            if filename_org and filename_locked:
                process_one_file(config=config, 
                                 source_dir=locked_dir, 
                                 target_dir=target_dir, 
                                 input_filename=filename_locked, 
                                 output_filename=filename_org)
                locked_file = os.path.join(locked_dir, filename_locked)
                processed_file = os.path.join(locked_dir, f"{filename_org}.processed")
                os.rename(locked_file, processed_file)
            else:
                # unlock the directory
                process_dot_cid_file = os.path.join(locked_dir, "process.cid")
                if os.path.exists(process_dot_cid_file):
                    os.remove(process_dot_cid_file)
                return

def main():
    parser = argparse.ArgumentParser(description="Assign CID to Zephir records.")
    parser.add_argument("--console", "-c", action="store_true", dest="console", help="display log entries on screen")
    parser.add_argument("--env", "-e", nargs="?", dest="env", choices=["test", "dev", "stg", "prd"], required=True, help="define runtime environment")
    parser.add_argument("--source_dir", "-s", nargs="?", dest="source_dir", help="source file directory")
    parser.add_argument("--target_dir", "-t", nargs="?", dest="target_dir", help="target file directroy")
    parser.add_argument("--infile", "-i", nargs="?", dest="input_filename", help="input filename")
    parser.add_argument("--outfile", "-o", nargs="?", dest="output_filename", help="output filename")
    parser.add_argument("--batch", "-b", action="store_true", dest="batch", help="assign CID in batch")
    parser.add_argument("--zephir_config", "-z",  nargs="?", dest="zephir_config", help="assign CID for specified config")

    args = parser.parse_args()

    console = args.console
    env = args.env
    source_dir = args.source_dir
    target_dir = args.target_dir
    input_filename = args.input_filename
    output_filename = args.output_filename
    batch = args.batch
    zephir_config = args.zephir_config

    ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(ROOT_PATH, "config")

    zephirdb_config = get_configs_by_filename(CONFIG_PATH, "zephir_db")
    zephirdb_conn_str = str(db_connect_url(zephirdb_config[env]))
    minterdb_conn_str = zephirdb_conn_str

    cid_minting_config = get_configs_by_filename(CONFIG_PATH, "cid_minting")

    primary_db_path = cid_minting_config["primary_db_path"]
    cluster_db_path = cid_minting_config["cluster_db_path"]
    logfile = cid_minting_config["logfile"]
    zephir_files_dir = cid_minting_config["zephir_files_dir"]
    zed_log_path = cid_minting_config["zed_log_path"]
    zed_msg_table = cid_minting_config["zed_msg_table"]

    pid = os.getpid()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    zed_log = os.path.join(zed_log_path, f"zed_cid_{timestamp}_{pid}")

    config = {
        "zephirdb_conn_str": zephirdb_conn_str,
        "minterdb_conn_str": minterdb_conn_str,
        "leveldb_primary_path": primary_db_path,
        "leveldb_cluster_path": cluster_db_path,
        "zed_log": zed_log,
        "zed_msg_table": zed_msg_table,
    }

    config_logger(logfile, console)

    logging.info("Start " + os.path.basename(__file__) + " PID:" + str(pid))
    logging.info("Env: {}".format(env))

    preparedfile_dir = os.path.join(zephir_files_dir, "*/prepared_files/")
    if zephir_config:
        preparedfile_dir = os.path.join(zephir_files_dir, f"{zephir_config}/prepared_files/")
        if not os.path.exists(preparedfile_dir):
            err_msg = f"Directory error: {preparedfile_dir} does not exist. Verify if {zephir_config} is a valid zephir config"
            logging.error(err_msg)
            print(err_msg)
            exit(1)

    if batch or zephir_config:
        batch_process(config, preparedfile_dir, pid)
    elif source_dir and target_dir and input_filename:
        if output_filename is None:
            output_filename = input_filename

        process_one_file(config, source_dir, target_dir, input_filename, output_filename)
    else:
        parser.print_help()
        exit(1)


    logging.info("Finished " + os.path.basename(__file__))
    print("For Testing: Finished " + os.path.basename(__file__))

if __name__ == '__main__':
    main()
