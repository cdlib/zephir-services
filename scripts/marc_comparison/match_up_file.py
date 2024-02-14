import argparse
import sys

from pymarc import parse_xml_to_array, Field
from collections import Counter


def get_value_from_location(record, loc):
    split = loc.split("$")
    current_value = record

    while len(split):
        if current_value is None:
            return None
        current_value = current_value.get(split.pop(0))

    if isinstance(current_value, Field):
        return current_value.value()

    return current_value


def compare_record(record1, record2, idloc1, idloc2):
    differences = Counter()

    def compare_field(location):
        r1_value = get_value_from_location(record1, location)
        r2_value = get_value_from_location(record2, location)

        if r1_value != r2_value:
            differences[location] = 1
    
    id1 = get_value_from_location(record1, idloc1)
    id2 = get_value_from_location(record2, idloc2)

    if id1 != id2:
        raise ValueError(f"IDs do not match: {id1} != {id2}")

    for field in ["260$a", "260$b", "260$c", "264$a", "264$b", "264$c"]:
        compare_field(field)
        
    if differences.total() > 0:
        print("HTID:", id1)
        for k, v in differences.items():
            print(f"First file: {k} = {get_value_from_location(record1, k)}")
            print(f"Second file: {k} = {get_value_from_location(record2, k)}")
        
    return differences


def marc_compare(file1, file2, idloc1, idloc2):
    print(file1)
    print(file2)
    
    records1 = load_records(file1)
    records2 = load_records(file2)

    len1 = len(records1)
    len2 = len(records2)
    if len1 != len2:
        raise ValueError(f"Number of records in {file1} ({len1}) does not match the number of records in {file2} ({len2}).")

    differences = Counter()
    
    for i in range(len(records1)):
        new_diffs = compare_record(records1[i], records2[i], idloc1, idloc2)

        differences.update(new_diffs)
    
    print(f"Number of records compared: {len1}")
    return 0

def load_records(file_path):
    # Load MARC records from an XML file
    with open(file_path, "r") as f:
        records = parse_xml_to_array(f)

    return records
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
    parser.add_argument("--idloc1", help="ID 1", required=True)
    parser.add_argument("--idloc2", help="ID 2", required=True)
    # Parse arguments
    args = parser.parse_args() 

    try:
        # run command with argument
        return_code = marc_compare(args.file1, args.file2, args.idloc1, args.idloc2)
    except Exception as err:
        print(err, file=sys.stderr)
        sys.exit(1) 

    sys.exit(return_code) 

if __name__ == "__main__":
    main()
