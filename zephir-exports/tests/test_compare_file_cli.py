import os
import sys

import pytest

from compare_file_cli import compare_file_cli


def test_compare_identical(td_tmpdir, capsys, pytestconfig):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = sys.argv = [
            "",
            os.path.join(td_tmpdir, "output-ref.json"),
            os.path.join(td_tmpdir, "output-ref-duplicate.json"),
            "--verbosity",
            pytestconfig.getoption("verbose") or 1,
        ]
        compare_file_cli()
    out, err = capsys.readouterr()
    print(out, file=sys.stdout)
    print(err, file=sys.stderr)
    assert "Differences start on line" not in err
    assert "No differences" in err
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]


def test_compare_different(td_tmpdir, capsys, pytestconfig):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = sys.argv = [
            "",
            os.path.join(td_tmpdir, "output-ref.json"),
            os.path.join(td_tmpdir, "output-ref-different.json"),
            "--verbosity",
            pytestconfig.getoption("verbose"),
        ]
        compare_file_cli()
    out, err = capsys.readouterr()
    print(out, file=sys.stdout)
    print(err, file=sys.stderr)
    assert "Differences start on line: 3" in err
    assert "No differences found" not in err
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]
