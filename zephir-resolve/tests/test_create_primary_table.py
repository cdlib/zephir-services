import os
import sys

import msgpack
import plyvel
import pytest

from resolution_table import ResolutionTable
from create_primary_table import create_primary_table


def test_create_primary_file(tmpdatadir, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = [
            "",
            os.path.join(tmpdatadir, "input.tsv"),
            os.path.join(tmpdatadir, "primary-table/"),
        ]
        create_primary_table()

    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]

    rt = ResolutionTable(os.path.join(tmpdatadir, "primary-table/"), key=int, value=int)
    assert 1 == rt.get(2)
    rt.close()
