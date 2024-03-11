import sys
import pytest
from example_cli import main # Ensure main is imported correctly

def test_example(td_tmpdir, capsys, pytestconfig):
    # Prepare the argument list for the CLI
    test_argument = "Example echo input"
    sys.argv = ["example_cli.py", test_argument]

    # Execute the CLI command, expecting it to handle the argument correctly
    with pytest.raises(SystemExit) as pytest_e:
        main()

    # Capture the output
    out, err = capsys.readouterr()

    # Output the captured output to the respective streams for debugging if necessary
    print(out, file=sys.stdout)
    print(err, file=sys.stderr)

    # Assert the expected output and exit code
    assert test_argument in out, "The CLI should echo the input argument"
    assert pytest_e.type == SystemExit, "The CLI should exit gracefully"
    assert pytest_e.value.code == 0, "The CLI should exit with a code of 0 indicating success"

