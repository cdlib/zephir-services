import os
import sys
import pytest
from id_printout import main

def test_printout_three_records(td_tmpdir, capsys, pytestconfig):
    xml_file = os.path.join(td_tmpdir, "three_records.xml")
    idloc = "974$u"
    sys.argv = ["id_printout.py", "--file", xml_file, "--idloc", idloc]

    # Execute the CLI command, expecting it to handle the argument correctly
    with pytest.raises(SystemExit) as pytest_e:
        main()

    # Capture the output
    out, err = capsys.readouterr()

    # Output the captured output to the respective streams for debugging if necessary
    print(out, file=sys.stdout)
    print(err, file=sys.stderr)

    # Assert the expected output and exit code
    # The number after mdp is added to differentiate the records
    expected_outputs = [
        "mdp1.39015018415946",
        "mdp2.39015018415946", 
        "mdp3.39015018415946"
    ]
    expected_actual = zip(expected_outputs, out.splitlines())
    for i, (expected, actual) in enumerate(expected_actual, start=1):
        assert expected == actual, f"Mismatch in record {i}!"
    assert pytest_e.type == SystemExit
    assert pytest_e.value.code == 0