import datetime
import filecmp
import os
import shutil
import sys
import zlib

from freezegun import freeze_time
import pytest

from ht_bib_export_incr import generate_export_incr
from export_cache import ExportCache


@pytest.fixture
def env_setup(td_tmpdir, monkeypatch):
    # use tmpdir configuration, export-path and cache-path.
    monkeypatch.setenv(
        "ZEPHIR_OVERRIDE_CONFIG_PATH", os.path.join(str(td_tmpdir), "config")
    )
    monkeypatch.setenv("ZEPHIR_EXPORT_PATH", td_tmpdir)
    monkeypatch.setenv("ZEPHIR_CACHE_PATH", td_tmpdir)


@freeze_time("2019-02-18")
def test_create_bib_export_incr(td_tmpdir, env_setup, capsys):
    for selection in ["v2", "v3"]:
        os.rename(
            os.path.join(td_tmpdir, "cache-{}-ref.db".format(selection)),
            os.path.join(
                td_tmpdir,
                "cache-{}-{}.db".format(
                    selection, datetime.datetime.today().strftime("%Y-%m-%d")
                ),
            ),
        )
        generate_export_incr(selection=selection, force=True)
        export_filename = "{}-ht_bib_export_incr_{}.json".format(
            selection, datetime.datetime.today().strftime("%Y-%m-%d")
        )
        assert filecmp.cmp(
            os.path.join(td_tmpdir, export_filename),
            os.path.join(td_tmpdir, "{}-ht_bib_export_incr_ref.json".format(selection)),
        )
