import os
import re
from pathlib import Path

import pymarc


def keep(filename):
    if filename.endswith(".org"):
        return False
    if os.path.exists("xml/" + filename[:-4] + "-fixed.xml"):
        return False
    return True


def extract_datetime(filename):
    """
    Some files have YYYYMMDDTTTT and some have YYYYMMDD,
    sorting (as strings) should work normally.
    """
    return re.search(r'\d+', filename).group()


def get_barcode(record):
    return record["955"]["b"]


def get_clean_ids(clean_ids_file):
    clean_ids = set()
    with open(clean_ids_file, "r") as f:
        for line in f:
            clean_ids.add(line.strip())
    return clean_ids


def main():
    xml_directory = Path("inputs/xml") # Expects a directory of XML files, each containing many MARC records.
    clean_ids_file = Path("inputs/clean_ids.txt") # File containing a list of clean IDs.
    exceptions_file = Path("outputs/exceptions.txt") # File to write exceptions to.
    output_file = Path("outputs/records.txt") # Output file to write the records to.

    filenames = [filename for filename in os.listdir(xml_directory) if keep(filename)]
    filenames.sort(key=extract_datetime)

    clean_ids = get_clean_ids(clean_ids_file)

    d = {}
    with open(exceptions_file, "w") as exceptions_f:
        for filename in filenames:
            with open(xml_directory / filename, "r", errors="replace") as f:
                try:
                    xml_file = pymarc.parse_xml_to_array(f)
                except Exception as e:
                    exceptions_f.write(f"{filename}: {e}. File read error.\n")
                    continue
                
            for i, record in enumerate(xml_file, start=1):
                try:
                    barcode = get_barcode(record)
                except Exception as e:
                    exceptions_f.write(f"{filename}: {e}. Barcode read error. Record number {i} in file.\n")
                    continue

                if barcode in clean_ids:
                    d[barcode] = record

    with open(output_file, "wb") as f:
        writer = pymarc.XMLWriter(f)
        for record in d.values():
            writer.write(record)
        writer.close()


if __name__ == "__main__":
    main()