import argparse
import sys

from pymarc import parse_xml_to_array

def marc_compare(file1, file2, id1, id2):
    print(file1)
    print(file2)
    
    len1 = load_and_count_records(file1)
    len2 = load_and_count_records(file2)

    if len1 != len2:
        raise ValueError(f"Number of records in {file1} ({len1}) does not match the number of records in {file2} ({len2}).")
    
    print(f"Number of records compared: {len1}")
    return 0

def load_and_count_records(file_path):
    # Load MARC records from an XML file
    with open(file_path, "r") as f:
        records = parse_xml_to_array(f)

    return len(records)
    # print(f"{len(records)=}")
    # # print(type(records[0].get("008")))
    # # print(records[0].get("008"))
    # # print(records[0].get("008").value())
    # # print(type(records[0].get("008").value()))
    # for record in records:
    #     print(f"{field}${subfield}: {record.get(field).get_subfields(subfield)}")

def main():
    parser = argparse.ArgumentParser(description="Compare two MARCXML files.")
    parser.add_argument("--file1", help="First MARC XML file to compare", required=True)
    parser.add_argument("--file2", help="Second MARC XML file to compare", required=True)
    parser.add_argument("--id1", help="ID 1", required=True)
    parser.add_argument("--id2", help="ID 2", required=True)
    # Parse arguments
    args = parser.parse_args() 

    try:
        # run command with argument
        return_code = marc_compare(args.file1, args.file2, args.id1, args.id2)
    except Exception as err:
        print(err, file=sys.stderr)
        sys.exit(1) 

    sys.exit(return_code) 

if __name__ == "__main__":
    main()

    # x = load_and_print_records(data/first.xml)