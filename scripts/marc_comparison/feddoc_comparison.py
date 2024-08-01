from pymarc import Field
import argparse
import sys
import csv

from pymarc import parse_xml_to_array

def get_value_from_location(record, loc):
    if loc == "LDR":
        return record.leader

    split = loc.split("$")
    current_value = record

    while len(split):
        if current_value is None:
            return None
        current_value = current_value.get(split.pop(0))

    if isinstance(current_value, Field):
        return current_value.value()

    return current_value

def compare_records(record1, record2):
    record1_008 = get_value_from_location(record1, "008")
    record2_008 = get_value_from_location(record2, "008")

    feddoc1 = record1_008[28] if record1_008 else None
    feddoc2 = record2_008[28] if record2_008 else None

    return feddoc1, feddoc2


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--file1", help="First file to compare", required=True)
    parser.add_argument("--file2", help="Second file to compare", required=True)
    parser.add_argument("--idloc1", help="Location of the ID in the first file", required=True)
    parser.add_argument("--idloc2", help="Location of the ID in the second file", required=True)
    parser.add_argument("--output", help="Output spreadsheet path", required=True)

    args = parser.parse_args()

    records1 = parse_xml_to_array(args.file1)
    records2 = parse_xml_to_array(args.file2)

    if len(records1) != len(records2):
        print("Files do not have the same number of records", file=sys.stderr)
        return

    with open(args.output, "w") as f:
        writer = csv.DictWriter(f, fieldnames=["ID", "File1 008/28", "File2 008/28", "Gained f"])

        writer.writeheader()
                
        for i in range(len(records1)):
            record1 = records1[i]
            record2 = records2[i]
            id1 = get_value_from_location(record1, args.idloc1)
            id2 = get_value_from_location(record2, args.idloc2)

            if id1 != id2:
                print(f"IDs do not match: {id1} {id2}", file=sys.stderr)
                continue

            feddoc1, feddoc2 = compare_records(record1, record2)

            gained_f = feddoc2 == "f" and feddoc1 != "f"
            writer.writerow(
                {
                    "ID": id1,
                    "File1 008/28": feddoc1,
                    "File2 008/28": feddoc2,
                    "Gained f": gained_f
                }
            )
            if gained_f:
                print(id1)
            

if __name__ == "__main__":
    main()
