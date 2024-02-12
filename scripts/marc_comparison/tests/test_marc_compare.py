from pathlib import Path
import os
import sys

import pytest
from marc_compare import main 

def test_example(td_tmpdir, capsys, pytestconfig):
    # Prepare the argument list for the CLI
    first_file = os.path.join(td_tmpdir, "first.xml")
    second_file = os.path.join(td_tmpdir, "second.xml")
    sys.argv = ["marc_compare.py", first_file, second_file]

    # Execute the CLI command, expecting it to handle the argument correctly
    with pytest.raises(SystemExit) as pytest_e:
        main()

    # Capture the output
    out, err = capsys.readouterr()

    # Output the captured output to the respective streams for debugging if necessary
    print(out, file=sys.stdout)
    print(err, file=sys.stderr)

    # Assert the expected output and exit code
    # assert first_file in out, ""
    # assert second_file in out, ""
    assert pytest_e.type == SystemExit
    assert pytest_e.value.code == 0

