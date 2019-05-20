import datetime
import filecmp
import os
import shutil
import sys
import zlib

from freezegun import freeze_time
import pytest

from export_cache import ExportCache
from generate_cli import generate_cli


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


def test_version_required(td_tmpdir, env_setup, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = sys.argv = [""]
        generate_cli()
    out, err = capsys.readouterr()
    assert "Error" in err
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 2]


@freeze_time("2019-02-18")
def test_exports_successfully(td_tmpdir, env_setup, capsys):
    test_sets = [["ht-bib-full", "v2", "full"], ["ht-bib-incr", "v3", "incr"]]
    for test_set in test_sets:
        export_type = test_set[0]
        version = test_set[1]
        name = test_set[2]
        # test create successful
        with pytest.raises(SystemExit) as pytest_e:
            sys.argv = sys.argv = ["", export_type, "--version", version, "--force"]
            generate_cli()
            out, err = capsys.readouterr()
            print(err)
            assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]
        # compare cache created to reference cache
        new_cache = ExportCache(
            td_tmpdir,
            "cache-{}-{}".format(
                version, datetime.datetime.today().strftime("%Y-%m-%d")
            ),
        )
        ref_cache = ExportCache(td_tmpdir, "cache-{}-ref".format(version))
        assert new_cache.size() == ref_cache.size()
        assert hash(new_cache.frozen_content_set()) == hash(
            ref_cache.frozen_content_set()
        )
        export_filename = "{}-ht_bib_export_{}_{}.json".format(
            version, name, datetime.datetime.today().strftime("%Y-%m-%d")
        )
        assert filecmp.cmp(
            os.path.join(td_tmpdir, export_filename),
            os.path.join(
                td_tmpdir, "{}-ht_bib_export_{}_ref.json".format(version, name)
            ),
        )
