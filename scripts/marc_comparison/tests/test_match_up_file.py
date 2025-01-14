import os
import sys
# import re
import pytest
from match_up_file import main

from match_up_file import get_value_from_location, parse_xml_to_array

def test_record_number_match(td_tmpdir, capsys, pytestconfig):
    # Prepare the argument list for the CLI
    first_file = os.path.join(td_tmpdir, "first.xml")
    second_file = os.path.join(td_tmpdir, "second.xml")
    first_id = "HOL$p"
    second_id = "974$u"
    sys.argv = ["match_up_file.py", "--file1", first_file, "--file2", second_file, "--idloc1", first_id, "--idloc2", second_id]

    # Execute the CLI command, expecting it to handle the argument correctly
    with pytest.raises(SystemExit) as pytest_e:
        main()

    # Capture the output
    out, err = capsys.readouterr()

    # Output the captured output to the respective streams for debugging if necessary
    print(out, file=sys.stdout)
    print(err, file=sys.stderr)

    # Assert the expected output and exit code
    assert f"{first_file}" in out, ""
    assert f"{second_file}" in out, ""
    assert "Number of records compared: 1" in out, ""
    assert pytest_e.type == SystemExit
    assert pytest_e.value.code == 0

def test_record_number_mismatch(td_tmpdir, capsys, pytestconfig):
    # Prepare the argument list for the CLI
    first_file = os.path.join(td_tmpdir, "first.xml")
    second_file = os.path.join(td_tmpdir, "third.xml")
    first_id = "HOL$p"
    second_id = "974$u"
    sys.argv = ["match_up_file.py", "--file1", first_file, "--file2", second_file, "--idloc1", first_id, "--idloc2", second_id]

    # Execute the CLI command, expecting it to handle the argument correctly
    with pytest.raises(SystemExit) as pytest_e:
        main()

    # Capture the output
    out, err = capsys.readouterr()

    # Output the captured output to the respective streams for debugging if necessary
    print(out, file=sys.stdout)
    print(err, file=sys.stderr)

    # Assert the expected output and exit code
    assert "does not match the number of records in" in err, ""
    assert pytest_e.type == SystemExit
    assert pytest_e.value.code == 1

def test_file_not_found(td_tmpdir, capsys, pytestconfig):
    # Prepare the argument list for the CLI
    first_file = os.path.join(td_tmpdir, "first.xml")
    second_file = os.path.join(td_tmpdir, "does_not_exist.xml")
    first_id = "HOL$p"
    second_id = "974$u"
    sys.argv = ["match_up_file.py", "--file1", first_file, "--file2", second_file, "--idloc1", first_id, "--idloc2", second_id]

    # Execute the CLI command, expecting it to handle the argument correctly
    with pytest.raises(SystemExit) as pytest_e:
        main()

    # Capture the output
    out, err = capsys.readouterr()

    # Output the captured output to the respective streams for debugging if necessary
    print(out, file=sys.stdout)
    print(err, file=sys.stderr)

    # Assert the expected output and exit code
    assert "No such file or directory: " in err, ""
    assert pytest_e.type == SystemExit
    assert pytest_e.value.code == 1


def test_get_record_location(td_tmpdir):
    records = parse_xml_to_array(os.path.join(td_tmpdir, "first.xml"))
    
    assert get_value_from_location(records[0], "245$a") == "Slovenské vyst̕ahovalectvo /"
    assert get_value_from_location(records[0], "001") == "mdp.39015018415946"
    assert get_value_from_location(records[0], "110") == None
    assert get_value_from_location(records[0], "110$a") == None


def test_record_ids_mismatch(td_tmpdir, capsys, pytestconfig):
    first_file = os.path.join(td_tmpdir, "first.xml")
    second_file = os.path.join(td_tmpdir, "second.xml")
    first_id = "245$a"
    second_id = "974$u"
    sys.argv = ["match_up_file.py", "--file1", first_file, "--file2", second_file, "--idloc1", first_id, "--idloc2", second_id]

    # Execute the CLI command, expecting it to handle the argument correctly
    with pytest.raises(SystemExit) as pytest_e:
        main()

    # Capture the output
    out, err = capsys.readouterr()

    # Output the captured output to the respective streams for debugging if necessary
    print(out, file=sys.stdout)
    print(err, file=sys.stderr)

    # Assert the expected output and exit code
    assert f"{first_file}" in out, ""
    assert f"{second_file}" in out, ""
    assert "IDs do not match" in err, ""
    assert pytest_e.type == SystemExit
    assert pytest_e.value.code == 1
