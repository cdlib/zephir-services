import os
import sys

import msgpack
import plyvel
import pytest

from resolution_table import ResolutionTable
from create_cluster_table import create_cluster_table


def test_create_cluster_file(tmpdatadir, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = [
            "",
            os.path.join(tmpdatadir, "input.tsv"),
            os.path.join(tmpdatadir, "cluster-table/"),
        ]
        create_cluster_table()

    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]

    rt = ResolutionTable(
        os.path.join(tmpdatadir, "cluster-table/"), key=int, value=list
    )
    assert [2, 3] == rt.get(1)
    rt.close()
