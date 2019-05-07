import datetime
import filecmp
import os
import shutil
import sys
import zlib

import pytest

from compare_cache_cli import compare_cache_cli


def test_files_required(td_tmpdir, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = sys.argv = [""]
        compare_cache_cli()
    out, err = capsys.readouterr()
    assert "Error" in err
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 2]


def test_compare_identical_files(td_tmpdir, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = sys.argv = ["", os.path.join(td_tmpdir,"cache_ref.db"), os.path.join(td_tmpdir,"cache_ref_duplicate.db")]
        compare_cache_cli()
    out, err = capsys.readouterr()
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]
