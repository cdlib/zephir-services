#!/usr/bin/env python
"""validate.py: Validate ZED log file to ensure all the data is JSON and
conforms to schemas

author: Charlie Collett"
copyright: Copyright 2018 The Regents of the University of California. All
rights reserved."""

import argparse
from collections import defaultdict
import json
import os
import sys

import jsonschema

from helpers.console_messenger import ConsoleMessenger


def main(argv=None):
    # Command line argument configuration
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath", nargs="*", help="Filepath to files for processing")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Emit messages dianostic messages about everything.",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Don't emit non-error messages to stderr. Errors are still emitted silence those with 2>/dev/null.",
    )
    parser.add_argument(
        "-d", "--dry-run", action="store_true", help="Run process without side-effects"
    )
    parser.add_argument(
        "-s",
        "--suffix",
        action="store",
        default="validated",
        help="for renaming valid files",
    )
    args = parser.parse_args()

    if len(args.filepath) == 0:
        print("No files given to process.", file=sys.stderr)
        sys.exit(1)

    # Print handler to manage when and how messages should print
    console = ConsoleMessenger(args.quiet, args.verbose)

    schema_file = os.path.join(os.path.dirname(__file__), 'config/zed_schema.json')
    print(schema_file)
    with open(schema_file, 'r') as f:
        schema_data = f.read()
        schema = json.loads(schema_data)

    if args.dry_run:
        console.diagnostic("DRY RUN")

    # Iterate over the json log files to process
    for file in args.filepath:

        if not os.path.isfile(file):
            console.error("File path '{0}' does not exist.".format(file))
            break

        # Get the file name, path, and create destination file name, path
        f_path, f_name = os.path.split(file)
        renamed_file = os.path.join("{0}.{1}".format(file, args.suffix))

        if os.path.isfile(renamed_file):
            console.error("Validated file '{0}' already exists.".format(renamed_file))
            break

        # Open file and validate
        with open(file) as f_io:
            event_counter = defaultdict(int)
            file_valid = True  # Assume valid until line found invalid
            ln_cnt = 0
            console.diagnostic("Validating: {}".format(file))
            for line in f_io:
                ln_cnt += 1

                # JSON VALIDATION BLOCK
                try:
                    event = json.loads(line.strip())
                    jsonschema.validate(event, schema)
                except json.decoder.JSONDecodeError:
                    file_valid = False
                    console.error("Invalid JSON on line {0}".format(ln_cnt))
                    break
                except jsonschema.exceptions.ValidationError:
                    file_valid = False
                    console.error("JSON Validation error on line {0}".format(ln_cnt))
                    break

                # DUPE-DETECTION BLOCK
                event_counter[event["event"]] += 1
                if event_counter[event["event"]] > 1:
                    file_valid = False
                    console.error(
                        "Duplicate ID ({0}) found on line {1} \
                        ".format(
                            event["event"], ln_cnt
                        )
                    )
                    break
            # Report results
            if file_valid is False:
                console.error("File {0}: invalid.".format(file))
            else:
                if not args.dry_run:
                    os.rename(file, renamed_file)
                console.report(
                    "File {0}: valid. {1} event(s) validated.".format(file, ln_cnt)
                )
    console.report("Done!")
    sys.exit(0)


if __name__ == "__main__":
    main()
