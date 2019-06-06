import datetime
import filecmp
import os
import shutil
import sys
import zlib

import pytest

from compare_cache_cli import compare_cache_cli


def test_compare_identical_files(td_tmpdir, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = sys.argv = [
            "",
            os.path.join(td_tmpdir, "cache-ref.db"),
            os.path.join(td_tmpdir, "cache-ref-duplicate.db"),
        ]
        compare_cache_cli()
    out, err = capsys.readouterr()
    assert not out and not err
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]


def test_compare_diff_with_diff_size(td_tmpdir, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = sys.argv = [
            "",
            os.path.join(td_tmpdir, "cache-ref.db"),
            os.path.join(td_tmpdir, "cache-ref-diff-with-diff-size.db"),
        ]
        compare_cache_cli()
    out, err = capsys.readouterr()
    assert "+(cid:102482345,key:1925062127)" in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]


def test_compare_diff_with_same_size(td_tmpdir, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = sys.argv = [
            "",
            os.path.join(td_tmpdir, "cache-ref.db"),
            os.path.join(td_tmpdir, "cache-ref-diff-with-same-size.db"),
        ]
        compare_cache_cli()
    out, err = capsys.readouterr()
    assert f"""-(cid:000393503,key:4043639245)
        -(cid:000434955,key:192506212)
        +(cid:000080471,key:270256059)
        +(cid:000434956,key:192506212)
        +cid:000393503,key:4043639233)"""
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]
