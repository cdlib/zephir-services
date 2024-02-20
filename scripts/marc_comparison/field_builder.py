import re
import argparse
import sys

import pymarc

from match_up_file import get_value_from_location

def valid_location(location):
    location_pattern = r"[A-Z0-9]{3}(?:\$[a-z0-9])?"
    match = re.fullmatch(location_pattern, location)
    return bool(re.fullmatch(location_pattern, location))


def update_value_at_location(record, location, value):
    if not valid_location(location):
        raise ValueError("Invalid location")

    split = location.split("$")
    tag = split[0]

    if len(split) == 1:
        # No subfield, must be a control field
        if int(tag) >= 10:
            raise ValueError("Invalid control field location")
        if tag not in record:
            record.add_field(pymarc.Field(tag, data=value))
        else:
            record[tag].data = value
    else:
        subfield = split[1]
        if tag not in record:
            record.add_ordered_field(pymarc.Field(
                tag=tag,
                indicators=[" ", " "],
                subfields=[pymarc.Subfield(code=subfield, value=value)]))
            return record
        else:
            record[tag][subfield] = value

    return record

def parse_pattern(pattern):
    location_pattern = r"[A-Z0-9]{3}(?:\$[a-z0-9])?"
    valid_pattern = r"({loc})=((?:{{{loc}}}|[^{{}}])+)".format(loc=location_pattern)

    match = re.fullmatch(valid_pattern, pattern)
    if not match:
        raise ValueError("Invalid pattern")

    outloc = match.group(1)
    pattern = match.group(2)

    inlocs = re.findall(fr"{{({location_pattern})}}", pattern)
    pattern = re.sub(fr"{{{location_pattern}}}", "{}", pattern)

    return inlocs, outloc, pattern

def process_file(input_file, output_file, pattern):
    inlocs, outloc, pattern = parse_pattern(pattern)

    with open(input_file, 'r') as infile:
        records = pymarc.parse_xml_to_array(infile)

    records_written = 0
    with open(output_file, 'wb') as outfile:
        writer = pymarc.XMLWriter(outfile)
        
        for record in records:
            invals = [get_value_from_location(record, loc) for loc in inlocs]

            if None in invals:
                raise ValueError(f"No value found at {inlocs[invals.index(None)]}")
            
            outval = pattern.format(*invals)
            record = update_value_at_location(record, outloc, outval)
            writer.write(record)

            records_written += 1
        writer.close()
    print(f"Number of records processed: {records_written}")
    return 0

    
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
