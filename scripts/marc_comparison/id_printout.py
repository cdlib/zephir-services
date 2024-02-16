import argparse
import sys

from pymarc import parse_xml_to_array

def get_field_value(record, field, subfield):
    return record.get(field).get_subfields(subfield)[0]

def id_printout(file, idloc):
    field, subfield = idloc.split("$")
    records = parse_xml_to_array(file)
    for record in records:
        print(get_field_value(record, field, subfield))
    return 0

def main():
    parser = argparse.ArgumentParser(description="Compare two MARCXML files.")
    parser.add_argument("--file", help="MARC XML file to process", required=True)
    parser.add_argument("--idloc", help="Location of ID field (e.g. 974$u)", required=True)
    # Parse arguments
    args = parser.parse_args() 

    try:
        # run command with argument
        return_code = id_printout(args.file, args.idloc)
    except Exception as err:
        print(err, file=sys.stderr)
        sys.exit(1) 

    sys.exit(return_code) 

if __name__ == "__main__":
    main()
