from collections import defaultdict
import json
import os
import sys

import click
import environs
import jsonschema

from lib.utils import ConsoleMessenger


@click.command()
@click.argument("filepath", nargs=-1, type=click.Path(exists=True))
@click.option(
    "-q", "--quiet", is_flag=True, default=False, help="Only emit error messages"
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=True,
    help="Emit messages dianostic messages about everything",
)
@click.option(
    "-d",
    "--dry-run",
    is_flag=True,
    default=False,
    help="Don't emit non-error messages to stderr. Errors are still emitted \
    silence those with 2>/dev/null.",
)
@click.option(
    "-s", "--suffix", "suffix", default="validated", help="for renaming valid files"
)
@click.option(
    "-i", "--invalid-suffix", "invalid_suffix", default="invalid", help="for renaming invalid files"
)
def validate(filepath, quiet, verbose, dry_run, suffix, invalid_suffix):
    """validate.py: Validate ZED log file to ensure all the data is JSON and
    conforms to schemas"""

    # Print handler to manage when and how messages should print
    console = ConsoleMessenger(quiet, verbose)

    # REQUIREMENTS
    if len(filepath) == 0:
        console.error("No files given to process.")
        sys.exit(1)

    # APPLICATION SETUP
    # load environment
    env = environs.Env()
    env.read_env()

    schema_file = os.path.join(os.path.dirname(__file__), "config/zed_schema.json")

    with open(schema_file, "r") as f:
        schema_data = f.read()
        schema = json.loads(schema_data)

    if dry_run:
        console.diagnostic("DRY RUN")

    # Iterate over the json log files to process
    for file in filepath:

        if not os.path.isfile(file):
            console.error("File path '{0}' does not exist.".format(file))
            break

        # Get the file name, path, and create destination file name, path
        f_path, f_name = os.path.split(file)
        renamed_file = f"{file}.{suffix}"

        invalid_file = f"{file}.{invalid_suffix}"

        event_counter = defaultdict(int)

        # Update event counter if renamed file exists
        if os.path.isfile(renamed_file):
            with open(renamed_file, 'r') as renamed_io:
                for line in renamed_io.readlines():
                    event = json.loads(line.strip())
                    event_counter[event["event"]] += 1
        
        # Open file and validate
        with open(file) as f_io:

            file_valid = True  # Assume valid until line found invalid
            ln_cnt = 0
            console.diagnostic("Validating: {}".format(file))

            lines = f_io.readlines()
            for line in lines:
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

                # Rename file
                os.rename(file, invalid_file)
            else:
                if not dry_run:
                    # Check if file already exists
                    if os.path.isfile(renamed_file):
                        # Remove file and append contents to renamed file
                        os.remove(file)
                        with open(renamed_file, 'a') as renamed_io:
                            renamed_io.writelines(lines)
                    else:
                        # Rename file
                        os.rename(file, renamed_file)
                console.report(
                    "File {0}: valid. {1} event(s) validated.".format(file, ln_cnt)
                )
    console.report("Done!")
    print(filepath)
    sys.exit(0)


if __name__ == "__main__":
    validate()
