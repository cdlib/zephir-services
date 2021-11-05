import os
import sys

import pytest

from example_cli import example_cli


def test_example(td_tmpdir, capsys, pytestconfig):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = sys.argv = [
            "",
            "Example echo input"
            # "--verbosity",
            # pytestconfig.getoption("verbose") or 1,
        ]
        example_cli()
    out, err = capsys.readouterr()
    print(out, file=sys.stdout)
    print(err, file=sys.stderr)
    assert "Example echo input" in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]
