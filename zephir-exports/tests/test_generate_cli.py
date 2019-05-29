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


def test_required_arguments_enforced(td_tmpdir, env_setup, capsys):
    incomplete_requirements = [
        {"args": ["", "ht-bib-full"], "error": "--merge-version"},
        {"args": ["", "--merge-version", "v3"], "error": "EXPORT_TYPE"},
    ]
    for req_set in incomplete_requirements:
        with pytest.raises(SystemExit) as pytest_e:
            sys.argv = req_set["args"]
            generate_cli()
        out, err = capsys.readouterr()
        assert req_set["error"] in err
        assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 2]


@freeze_time("2019-02-18")
def test_exports_complete(td_tmpdir, env_setup, capsys):
    arg_sets = [
        {"export-type": "ht-bib-full", "merge-version": "v2"},
        {"export-type": "ht-bib-incr", "merge-version": "v3"},
    ]
    for arg_set in arg_sets:
        with pytest.raises(SystemExit) as pytest_e:
            sys.argv = sys.argv = [
                "",
                arg_set["export-type"],
                "--merge-version",
                arg_set["merge-version"],
                "--force",
            ]
            generate_cli()
            assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]
        # compare cache created to reference cache
        new_cache = ExportCache(
            td_tmpdir,
            "cache-{}-{}".format(
                merge_version, datetime.datetime.today().strftime("%Y-%m-%d")
            ),
        )
        ref_cache = ExportCache(td_tmpdir, "cache-{}-ref".format(merge_version))
        assert new_cache.size() == ref_cache.size()
        assert hash(new_cache.frozen_content_set()) == hash(
            ref_cache.frozen_content_set()
        )
        export_filename = "ht_bib_export_{}_{}.json".format(
            name, datetime.datetime.today().strftime("%Y-%m-%d")
        )
        assert filecmp.cmp(
            os.path.join(td_tmpdir, export_filename),
            os.path.join(
                td_tmpdir, "{}-ht_bib_export_{}_ref.json".format(merge_version, name)
            ),
        )
