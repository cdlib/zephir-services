import filecmp
import os
import sys

import pytest

from create_primary_only_list import create_primary_only_list


def test_create_primary_only_list(tmpdatadir, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = [
            "",
            os.path.join(tmpdatadir, "concordance.txt"),
            os.path.join(tmpdatadir, "output.tsv"),
        ]
        create_primary_only_list()
        assert filecmp.cmp(
            os.path.join(tmpdatadir, "output.tsv"),
            os.path.join(tmpdatadir, "test_output.tsv"),
        )
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]
