import filecmp
import os
import sys

import pytest

from create_cluster_file import create_cluster_file


def test_create_cluster_file(tmpdatadir, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = [
            "",
            os.path.join(tmpdatadir, "input.tsv"),
            os.path.join(tmpdatadir, "output.tsv"),
        ]
        create_cluster_file()

    assert filecmp.cmp(
        os.path.join(tmpdatadir, "output.tsv"),
        os.path.join(tmpdatadir, "test_output.tsv"),
    )
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]
