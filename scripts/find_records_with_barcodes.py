import argparse
import logging
import pymarc
import sys
from contextlib import nullcontext

logger = logging.getLogger()

def open_input(path):
    if path == "-":
        return nullcontext(sys.stdin)
    return open(path, "r", encoding="utf-8", errors="replace")

def get_barcode(record):
    return record["955"]["b"]


def get_barcodes(barcodes_file):
    barcodes = set()
    with open_input(barcodes_file) as f:
        for line in f:
            barcodes.add(line.strip())
    return barcodes


def find_records(xml_files, barcodes_file):
    barcodes = get_barcodes(barcodes_file)

    records_by_barcode = {}
    with open_input(xml_files) as f:
        for line in f:
            xml_file_path = line.strip()
            try:
                records = pymarc.parse_xml_to_array(xml_file_path)
            except Exception as e:
                logger.error(f"Error reading file {xml_file_path}: {e}")
                continue

            for i, record in enumerate(records, start=1):
                try:
                    barcode = get_barcode(record)
                except Exception as e:
                    logger.error(f"Could not read barcode for record {i} in {xml_file_path}: {e}")
                    continue

                if barcode in barcodes:
                    records_by_barcode[barcode] = record

    # Write records to stdout in MARC XML format
    writer = pymarc.XMLWriter(sys.stdout.buffer)
    for record in records_by_barcode.values():
        writer.write(record)

    # Adds closing </collection> tag to the output, but does not close sys.stdout
    writer.close(close_fh=False)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Find the latest MARC record for each requested barcode in an ordered list of XML files."
        )
    )
    parser.add_argument(
        "--xml-file-list",
        required=True,
        help=(
            "Path to file (or - for stdin) containing paths to MARC XML file(s) to search. The files are processed in the order they are listed, and later records replace earlier records with the same barcode."
        ),
    )
    parser.add_argument(
        "--barcodes-file",
        required=True,
        help="Path to file (or - for stdin) containing one barcode per line.",
    )
    args = parser.parse_args(argv)
    if args.xml_file_list == "-" and args.barcodes_file == "-":
        parser.error("--xml-file-list and --barcodes-file cannot both read from stdin")
    return args


def main(argv=None):
    args = parse_args(argv)
    find_records(
        args.xml_file_list,
        args.barcodes_file
    )


if __name__ == "__main__":
    main()
