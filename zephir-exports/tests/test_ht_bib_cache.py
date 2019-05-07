import datetime
import os
import shutil
import sys
import zlib

import pytest

from ht_bib_cache import generate_cache
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


def test_create_cache_successfully(td_tmpdir, env_setup, capsys):
    for selection in ["v2", "v3"]:

        generate_cache(selection=selection, force=True)

        new_cache = ExportCache(
            td_tmpdir,
            "cache-{}-{}".format(
                selection, datetime.datetime.today().strftime("%Y-%m-%d")
            ),
        )
        ref_cache = ExportCache(td_tmpdir, "cache-{}-ref".format(selection))
        assert new_cache.size() == ref_cache.size()
        assert new_cache.content_hash() == ref_cache.content_hash()
