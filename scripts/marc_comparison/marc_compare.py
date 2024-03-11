import argparse
import sys  

from pymarc import parse_xml_to_array

def marc_compare(first_file, second_file):
  print("Printing records from the first file:")
  load_and_print_records(first_file)
  print("Printing records from the second file:")
  load_and_print_records(second_file)
  return 0

def load_and_print_records(file_path):
    # Load MARC records from an XML file
    records = parse_xml_to_array(file_path)
    # Print each record
    for record in records:
        print(record)
        print("=====================================")

def main():
    parser = argparse.ArgumentParser(description="Compare two MARCXML files.")
    parser.add_argument("first_file", help="First MARC xml file to compare")
    parser.add_argument("second_file", help="Second MARC xml file to compare")
    # Parse arguments
    args = parser.parse_args() 

    try:
      # run command with argument
      return_code = marc_compare(args.first_file, args.second_file)
    except Exception as err:
      print(err, file=sys.stderr)
      sys.exit(1) 

    sys.exit(return_code) 


if __name__ == "__main__":
    main()