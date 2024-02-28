import sys
from pathlib import Path

import pytest
from pymarc import parse_xml_to_array
from unittest.mock import mock_open, patch
import yaml

from export_marc_item_records import main
from export_marc_item_records import initialize_database_session


def test_export_item_records_success(td_tmpdir, capsys):
    # Create a temporary directory and files for the test
    input_file = Path(td_tmpdir) / "success_input.txt"
    output_file = Path(td_tmpdir) / "output_records.xml"
    config_file = Path(td_tmpdir) / "db.yml"

    # Prepare the argument list for the CLI
    sys.argv = [
        "export_item_records.py",
        "-f",
        str(input_file),
        "-o",
        str(output_file),
        "--db_config_path",
        str(config_file),
        "--db_env",
        "test",
    ]

    # Execute the CLI command
    with pytest.raises(SystemExit) as pytest_e:
        main()

    # Capture the output
    out, err = capsys.readouterr()

    # Output the captured output
    print(out, file=sys.stdout)
    print(err, file=sys.stderr)

    # Read output file and make sure it has 5 records using pymarc
    assert output_file.exists(), "Output file should exist"

    # Parse the MARCXML file and return the records as an array
    records = parse_xml_to_array(output_file)

    # simultaneously iterate the input file ids and records in the output file, match id on the 974$u
    input_ids = input_file.read_text().splitlines()
    for record, id in zip(records, input_ids):
        assert (
            record.get_fields("974")[0].get_subfields("u")[0] == id
        ), f"Record with 974$u {record.get_fields('974')[0].get_subfields('u')[0]} should match the input id {id}"

    # Assert that the number of records is as expected
    assert (
        len(records) == 5
    ), f"Output file should contain 5 MARCXML records, but found {len(records)}"

    # Assert that the expected message about successfully processed records is in the output
    assert "5 records exported" in err, "Export success message should be in the output"

    # Assert the script exits gracefully
    assert pytest_e.type == SystemExit, "The CLI should exit gracefully"
    assert (
        pytest_e.value.code == 0
    ), "The CLI should exit with a code of 0 indicating success"


def test_export_item_records_with_missing_ids(td_tmpdir, capsys):
    # Create a temporary directory and files for the test
    input_file = Path(td_tmpdir) / "missing_input.txt"
    output_file = Path(td_tmpdir) / "output_records_missing.xml"
    config_file = Path(td_tmpdir) / "db.yml"

    # Prepare the argument list for the CLI
    sys.argv = [
        "export_item_records.py",
        "-f",
        str(input_file),
        "-o",
        str(output_file),
        "--db_config_path",
        str(config_file),
        "--db_env",
        "test",
    ]

    # Execute the CLI command
    with pytest.raises(SystemExit) as pytest_e:
        main()

    # Capture the output
    out, err = capsys.readouterr()

    # Assert that the script reports the correct number of missing IDs
    # Assuming 2 IDs were missing
    assert (
        "IDs not found in the database" in err
    ), "Missing IDs warning should be in the error output"
    assert (
        len(err.split(",")) == 2
    ), "There should be 2 missing IDs reported in the error output"

    # The rest of the test can follow the structure of the success test, adjusting assertions as necessary
    # to account for the expected number of records that were successfully exported

    # Assert the script exits gracefully
    assert pytest_e.type == SystemExit, "The CLI should exit gracefully"
    assert (
        pytest_e.value.code == 0
    ), "The CLI should exit with a code of 0 indicating success"


def test_export_item_records_with_missing_input(td_tmpdir, capsys):
    # Create a temporary directory and files for the test
    input_file = Path(td_tmpdir) / "does_not_exit.txt"
    output_file = Path(td_tmpdir) / "output_wont_work.xml"
    config_file = Path(td_tmpdir) / "db.yml"

    # Prepare the argument list for the CLI
    sys.argv = [
        "export_item_records.py",
        "-f",
        str(input_file),
        "-o",
        str(output_file),
        "--db_config_path",
        str(config_file),
        "--db_env",
        "test",
    ]
    # Execute the CLI command
    with pytest.raises(SystemExit) as pytest_e:
        main()

    # Capture the output
    out, err = capsys.readouterr()

    # Assert the script exits gracefully
    assert pytest_e.type == SystemExit, "The CLI should exit gracefully"
    assert (
        pytest_e.value.code == 1
    ), "The CLI should exit with a code of 1 indicating failure"

def test_missing_config_file():
        with pytest.raises(SystemExit) as pytest_e:
            initialize_database_session(db_config_path="nonexistent_db.yml")
        assert str(pytest_e.value) == "1", "The script should exit with status code 1 for missing config file"
