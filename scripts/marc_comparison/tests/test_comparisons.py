import os
import sys

import pytest
from match_up_file import main

def test_no_differences(td_tmpdir, capsys, pytestconfig):
    # Prepare the argument list for the CLI
    first_file = os.path.join(td_tmpdir, "first.xml")
    second_file = os.path.join(td_tmpdir, "first.xml")
    first_id = "HOL$p"
    second_id = "974$u"
    sys.argv = ["match_up_id.py", "--file1", first_file, "--file2", second_file, "--idloc1", first_id, "--idloc2", second_id]

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


def test_differences(td_tmpdir, capsys, pytestconfig):
        # Prepare the argument list for the CLI
    first_file = os.path.join(td_tmpdir, "first.xml")
    second_file = os.path.join(td_tmpdir, "first_changed_pub.xml")
    first_id = "HOL$p"
    second_id = "974$u"
    sys.argv = ["match_up_id.py", "--file1", first_file, "--file2", second_file, "--idloc1", first_id, "--idloc2", second_id]

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
    assert "HTID: mdp.39015018415946" in out, ""
    assert "First file: 264$a = Martin :" in out, ""
    assert "Second file: 264$a = Martini :" in out, ""
    assert "Number of records compared: 1" in out, ""
    assert pytest_e.type == SystemExit
    assert pytest_e.value.code == 0
