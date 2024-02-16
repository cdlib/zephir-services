import re
import argparse
import sys

import pymarc

from match_up_file import get_value_from_location

def parse_pattern(pattern):
    location_pattern = r"[0-9]{3}(?:\$[a-z])?"
    valid_pattern = r"({loc})=((?:{{{loc}}}|[^{{}}])+)".format(loc=location_pattern)

    match = re.fullmatch(valid_pattern, pattern)
    if not match:
        raise ValueError("Invalid pattern")

    outloc = match.group(1)
    pattern = match.group(2)

    inlocs = re.findall(fr"{{({location_pattern})}}", pattern)
    pattern = re.sub(fr"{location_pattern}", "", pattern)

    return inlocs, outloc, pattern

def process_file(input_file, output_file, pattern):
    inlocs, outloc, pattern = parse_pattern(pattern)

    with open(input_file, 'r') as infile:
        records = pymarc.parse_xml_to_array(infile)

    with open(output_file, 'wb') as outfile:
        for record in records:
            invals = [get_value_from_location(record, loc) for loc in inlocs]

            if None in invals:
                raise ValueError(f"No value found at {inlocs[invals.index(None)]}")
            
            outval = pattern.format(*invals)
            
            
def main():
    parser = argparse.ArgumentParser(description="Field Builder")
    parser.add_argument("-i", "--input", help="Input File", required=True)
    parser.add_argument("-o", "--output", help="Output File", required=True)
    parser.add_argument("-p", "--pattern", help="Builder Pattern", required=True)
    args = parser.parse_args()

    try:
        return_code = process_file(args.input, args.output, args.pattern)
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    sys.exit(return_code)

if __name__ == "__main__":
    main()
