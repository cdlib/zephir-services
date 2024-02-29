import os
import sys

import pytest
import pymarc
from field_builder import main, parse_pattern, update_value_at_location

def test_parse_pattern():
    with pytest.raises(ValueError) as err:
        parse_pattern("001=")
    assert str(err.value) == "Invalid pattern"

    with pytest.raises(ValueError) as err:
        parse_pattern("001={245$a$b}")
    assert str(err.value) == "Invalid pattern"

    with pytest.raises(ValueError) as err:
        parse_pattern("8={245$a}")
    assert str(err.value) == "Invalid pattern"

    with pytest.raises(ValueError) as err:
        parse_pattern("")
    assert str(err.value) == "Invalid pattern"

    assert parse_pattern("001=hvd.{245$a}") == (["245$a"], "001", "hvd.{}")
    assert parse_pattern("245$a={245$a} / {245$b} / {245$c}") == (["245$a", "245$b", "245$c"], "245$a", "{} / {} / {}")
    assert parse_pattern("245$a=245$a: {245$a}") == (["245$a"], "245$a", "245$a: {}")

def test_update_value_at_location(td_tmpdir):
    record = pymarc.parse_xml_to_array(os.path.join(td_tmpdir, "first.xml"))[0]

    with pytest.raises(ValueError) as err:
        update_value_at_location(record, "001$", "test")
    assert str(err.value) == "Invalid location"

    with pytest.raises(ValueError) as err:
        update_value_at_location(record, "", "test")
    assert str(err.value) == "Invalid location"

    with pytest.raises(ValueError) as err:
        update_value_at_location(record, "245", "test")
    assert str(err.value) == "Invalid control field location"

    r = update_value_at_location(record, "245$a", "test")
    assert r["245"]["a"] == "test"

    r = update_value_at_location(record, "035$a", "test")
    assert r["035"]["a"] == "test"

    r = update_value_at_location(record, "260$b", "test")
    assert r["260"]["b"] == "test"

def test_cli_output(td_tmpdir, capsys, pytestconfig):
    input_file = os.path.join(td_tmpdir, "first.xml")
    output_file = os.path.join(td_tmpdir, "output.xml")

    pattern = "974$u=prefix.{001}"
    sys.argv = ["field_builder.py", "--input", input_file, "--output", output_file, "--pattern", pattern]

        # Execute the CLI command, expecting it to handle the argument correctly
    with pytest.raises(SystemExit) as pytest_e:
        main()

    # Capture the output
    out, err = capsys.readouterr()

    # Output the captured output to the respective streams for debugging if necessary
    print(out, file=sys.stdout)
    print(err, file=sys.stderr)

    assert pytest_e.type == SystemExit
    assert pytest_e.value.code == 0

    # Check that output record has correct 974$u
    record = pymarc.parse_xml_to_array(output_file)[0]
    assert record["974"]["u"] == "prefix.mdp.39015018415946"
