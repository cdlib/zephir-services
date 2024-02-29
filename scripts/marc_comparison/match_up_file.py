import argparse
import sys

from pymarc import parse_xml_to_array, Field
from collections import Counter


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


def nested_counter_total(counter):
    total = 0
    for k, v in counter.items():
        if isinstance(v, int):
            total += v
        elif isinstance(v, Counter):
            total += nested_counter_total(v)
        else:
            raise ValueError(f"Unexpected type: {type(v)}")
    return total


def nested_counter_update(counter, other):
    for k, v in other.items():
        if isinstance(v, int):
            counter[k] += v
        elif isinstance(v, Counter):
            if k not in counter:
                counter[k] = Counter()
            counter[k] = nested_counter_update(counter[k], v)
        else:
            raise ValueError(f"Unexpected type: {type(v)}")
    return counter


def get_OCLCs(record):
    """Extracts OCLC numbers from a MARC record.

    Given a MARC record, returns a list of tuples, each containing an OCLC
    prefix and number found in any 035$a fields of the record.

    Args:
        record (pymarc.Record): The MARC record to extract OCLC numbers from.

    Returns:
        A list of 2-tuples, like (prefix, number). An empty list is returned
        if no OCLC numbers are found.
    """
    
    # Valid OCLC prefixes (lowercased for case insensitivity)
    oclc_prefixes = list(map(str.lower, [
        "(OCoLC)",
        "(OCoLC)ocm",
        "(OCoLC)ocn",
        "(OCoLC)on",
        "(OCLC)",
        "(OCLC)ocm",
        "(OCLC)ocn",
        "(OCLC)on",
        "ocm",
        "ocn",
        "on"
    ]))
    # OCLCs found in 035$a fields
    oclcs = []

    # Get all 035 fields
    fields = record.get_fields('035')
    
    for field in fields:
        a = field.get('a')

        # If no $a, skip this 035
        if a is None:
            continue
        
        # Remove punctuation, except ()
        a = a.translate(str.maketrans('', '', ' !"#$%&\'*+,-./:;<=>?@[\\]^_`{|}~'))
        
        # Keep track of which prefixes have been matched
        prefixes_matched = []
        i = 0
        while (i < len(a) and
               any(map(lambda s: s.startswith(a[:i].lower()), oclc_prefixes))):
            if a[:i].lower() in oclc_prefixes:
                prefixes_matched.append(a[:i])
            i += 1

        if prefixes_matched:
            # Order prefixes by length to find the longest match
            prefixes_matched = sorted(prefixes_matched, key=lambda prefix: len(prefix), reverse=True)
            prefix = prefixes_matched[0]
            num = a[len(prefix):]

            if num.isdigit():
                oclcs.append((prefix, num))
    return oclcs


def get_primary_OCLC(record):
    oclcs = get_OCLCs(record) 
    return oclcs[0][1] if oclcs else None


def compare_record(record1, record2, idloc1, idloc2):
    differences = Counter()

    def compare_OCLC():
        r1_value = get_primary_OCLC(record1)
        r2_value = get_primary_OCLC(record2)

        if r1_value != r2_value:
            differences["OCLC"] = 1

    def compare_field(location, section=slice(None)):
        r1_value = get_value_from_location(record1, location)
        r2_value = get_value_from_location(record2, location)

        r1_value = r1_value[section] if r1_value else r1_value
        r2_value = r2_value[section] if r2_value else r2_value

        if section != slice(None):
            if location not in differences:
                differences[location] = Counter()
            if r1_value != r2_value:
                differences[location][f"{section.start}-{section.stop - 1}"] = 1
        else:
            if r1_value != r2_value:
                differences[location] = 1
    
    id1 = get_value_from_location(record1, idloc1)
    id2 = get_value_from_location(record2, idloc2)

    if id1 != id2:
        raise ValueError(f"IDs do not match: {id1} != {id2}")

    fields = [
        ("260$a",), ("260$b",), ("260$c",), ("264$a",), ("264$b",), ("264$c",),
        ("008", slice(7, 11)), ("008", slice(11, 15)), ("008", slice(15, 18)), ("008", slice(28, 29)), ("008", slice(17, 18)),
        ("LDR", slice(6, 8)),
        ("HOL$1",)
    ]

    for args in fields:
        compare_field(*args)
        
    compare_OCLC()

    if nested_counter_total(differences) > 0:
        print("HTID:", id1)
        for k, v in differences.items():
            if k == "oclc":
                print(f"File 1: OCLC = {get_primary_OCLC(record1)}")
                print(f"File 2: OCLC = {get_primary_OCLC(record2)}")
            else:
                print(f"File 1: {k} = {get_value_from_location(record1, k)}")
                print(f"File 2: {k} = {get_value_from_location(record2, k)}")

    return differences


def marc_compare(file1, file2, idloc1, idloc2):    

    records1 = load_records(file1)
    records2 = load_records(file2)

    len1 = len(records1)
    len2 = len(records2)
    if len1 != len2:
        raise ValueError(f"Number of records in {file1} ({len1}) does not match the number of records in {file2} ({len2}).")
    
    print(f"File 1: {file1}")
    print(f"File 2: {file2}\n")

    differences = Counter()
    num_records_with_differences = 0
    for i in range(len(records1)):
        new_diffs = compare_record(records1[i], records2[i], idloc1, idloc2)

        if nested_counter_total(new_diffs) > 0:
            num_records_with_differences += 1

        differences = nested_counter_update(differences, new_diffs)

    # Print summary of differences
    if num_records_with_differences > 0:
        print("")
        
    print(f"Number of records compared: {len1}")
    print(f"Number of records with differences: {num_records_with_differences}\n")
    
    print("Difference Counts:\n")

    print("LDR:", differences["LDR"]["6-7"])
    print("Bib ID:", differences["HOL$1"])
    print("008/07-10 (Date 1):", differences["008"]["7-10"])
    print("008/11-14 (Date 2):", differences["008"]["11-14"])
    print("008/15-17 (Pub Place):", differences["008"]["15-17"])
    print("008/28 (govdoc code f):", differences["008"]["28-28"])
    print("008/17:", differences["008"]["17-17"])
    print("OCLC:", differences["OCLC"])
    print("260$a (place of pub):", differences["260$a"])
    print("260$b (name of pub):", differences["260$b"])
    print("260$c (date of pub):", differences["260$c"])
    print("264$a (place of pub):", differences["264$a"])
    print("264$b (name of pub):", differences["264$b"])
    print("264$c (date of pub):", differences["264$c"])
    
    return 0

def load_records(file_path):
    # Load MARC records from an XML file
    with open(file_path, "r") as f:
        records = parse_xml_to_array(f)
    return records

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
