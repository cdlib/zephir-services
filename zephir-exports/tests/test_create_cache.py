import datetime
import os
import shutil
import sys
import zlib

import pytest

from create_cache import create_cache
from export_cache import ExportCache


@pytest.fixture
def env_setup(td_tmpdir, monkeypatch):
    monkeypatch.setenv(
        "ZEPHIR_OVERRIDE_CONFIG_PATH", os.path.join(str(td_tmpdir), "config")
    )
    monkeypatch.setenv("ZEPHIR_CACHE_PATH", os.path.join(str(td_tmpdir)))
    if "MYSQL_UNIX_PORT" in os.environ:
        monkeypatch.setenv("ZEPHIR_DB_SOCKET", os.environ["MYSQL_UNIX_PORT"])
    os.system("mysql --host=localhost --user=root  < {}/micro-db.sql".format(td_tmpdir))


def test_selection_required(td_tmpdir, env_setup, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = sys.argv = [""]
        create_cache()
    out, err = capsys.readouterr()
    assert "Error" in err
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 2]


def test_create_cache_successfully(td_tmpdir, env_setup, capsys):
    for selection in ["v2", "v3"]:
        # test create successful
        with pytest.raises(SystemExit) as pytest_e:
            sys.argv = sys.argv = ["", "--selection", selection, "--force"]
            create_cache()
            out, err = capsys.readouterr()
            assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]
        # compare cache created to reference cache
        new_cache = ExportCache(
            td_tmpdir,
            "cache-{}-{}".format(
                selection, datetime.datetime.today().strftime("%Y-%m-%d")
            ),
        )
        ref_cache = ExportCache(td_tmpdir, "cache-{}-ref".format(selection))
        assert new_cache.size() == ref_cache.size()
        assert new_cache.content_hash() == ref_cache.content_hash()
