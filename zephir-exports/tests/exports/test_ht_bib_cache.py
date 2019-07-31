import datetime
import os

import pytest

from exports.ht_bib_cache import ht_bib_cache
from lib.export_cache import ExportCache
from lib.utils import ConsoleMessenger


@pytest.fixture
def env_setup(td_tmpdir, monkeypatch):
    monkeypatch.setenv(
        "ZEPHIR_OVERRIDE_CONFIG_PATH", os.path.join(str(td_tmpdir), "config")
    )
    monkeypatch.setenv("ZEPHIR_CACHE_PATH", os.path.join(str(td_tmpdir)))
    if "MYSQL_UNIX_PORT" in os.environ:
        monkeypatch.setenv("ZEPHIR_DB_SOCKET", os.environ["MYSQL_UNIX_PORT"])
    os.system("mysql --host=localhost --user=root  < {}/micro-db.sql".format(td_tmpdir))


def test_create_cache_successfully(td_tmpdir, env_setup, capsys, pytestconfig):
    for merge_version in ["v2", "v3"]:

        console = ConsoleMessenger(verbosity=pytestconfig.getoption("verbose"))
        ht_bib_cache(console=console, merge_version=merge_version, force=True)

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


def test_reuse_existing_cache(td_tmpdir, env_setup, capsys, pytestconfig):
    console = ConsoleMessenger(verbosity=2)
    ht_bib_cache(console=console, merge_version="v3", force=False)
    out, err = capsys.readouterr()
    assert "Using existing cache" not in err
    ht_bib_cache(console=console, merge_version="v3", force=False)
    out, err = capsys.readouterr()
    assert "Using existing cache" in err
