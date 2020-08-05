import filecmp
import os
import sys

import pytest

from concat_files import concat_files


def test_concat_files_cli(tmpdatadir, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = [
            "",
            os.path.join(tmpdatadir, "file1.tsv"),
            os.path.join(tmpdatadir, "file2.tsv"),
            os.path.join(tmpdatadir, "output.tsv"),
        ]
        concat_files()

    assert filecmp.cmp(
        os.path.join(tmpdatadir, "output.tsv"),
        os.path.join(tmpdatadir, "test_output.tsv"),
    )
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]
