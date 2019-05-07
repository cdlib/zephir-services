import datetime
import filecmp
import os
import shutil
import sys
import zlib


import pytest

from export_cache import ExportCache
from ht_bib_cli import ht_bib_cli


@pytest.fixture
def env_setup(td_tmpdir, monkeypatch):
    # use tmpdir configuration, export-path and cache-path.
    monkeypatch.setenv(
        "ZEPHIR_OVERRIDE_CONFIG_PATH", os.path.join(str(td_tmpdir), "config")
    )
    monkeypatch.setenv("ZEPHIR_EXPORT_PATH", td_tmpdir)
    monkeypatch.setenv("ZEPHIR_CACHE_PATH", td_tmpdir)

    if "MYSQL_UNIX_PORT" in os.environ:
        monkeypatch.setenv("ZEPHIR_DB_SOCKET", os.environ["MYSQL_UNIX_PORT"])
    # load test data
    os.system("mysql --host=localhost --user=root  < {}/micro-db.sql".format(td_tmpdir))


def test_selection_required(td_tmpdir, env_setup, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = sys.argv = [""]
        ht_bib_cli()
    out, err = capsys.readouterr()
    assert "Error" in err
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 2]


def test_create_cache_successfully(td_tmpdir, env_setup, capsys):
    for selection in ["v2", "v3"]:
        # test create successful
        with pytest.raises(SystemExit) as pytest_e:
            sys.argv = sys.argv = [
                "",
                "--selection",
                selection,
                "--export-type",
                "full",
                "--force",
            ]
            ht_bib_cli()
            out, err = capsys.readouterr()
            print(err)
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
        export_filename = "{}-ht_bib_export_full_{}.json".format(
            selection, datetime.datetime.today().strftime("%Y-%m-%d")
        )
        assert filecmp.cmp(
            os.path.join(td_tmpdir, export_filename),
            os.path.join(td_tmpdir, "{}-ht_bib_export_full_ref.json".format(selection)),
        )
