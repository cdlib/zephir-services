import os
import sys

import pytest

from echo_cli import echo_cli

# Echo test: run command and check stdout
def test_echo(td_tmpdir, capsys, pytestconfig):
    # run echo cli
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = sys.argv = ["", "Example echo input"]
        echo_cli()

    # capture output and error for tests
    out, err = capsys.readouterr()

    # print cli stderr and stdout
    print(out, file=sys.stdout)
    print(err, file=sys.stderr)

    # check contents directly
    assert "Example echo input\n" == out
    # check contents with fixture in test_echo_cli dir
    with open(os.path.join(td_tmpdir, "echo.txt")) as file:
        assert file.read() == out

    # check command exited without error code d
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]
