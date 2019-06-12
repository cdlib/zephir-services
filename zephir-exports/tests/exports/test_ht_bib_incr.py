import datetime
import filecmp
import os

from freezegun import freeze_time
import pytest

from exports.ht_bib_incr import ht_bib_incr
from lib.utils import ConsoleMessenger


@pytest.fixture
def env_setup(td_tmpdir, monkeypatch):
    # use tmpdir configuration, export-path and cache-path.
    monkeypatch.setenv(
        "ZEPHIR_OVERRIDE_CONFIG_PATH", os.path.join(str(td_tmpdir), "config")
    )
    monkeypatch.setenv("ZEPHIR_OUTPUT_PATH", td_tmpdir)
    monkeypatch.setenv("ZEPHIR_CACHE_PATH", td_tmpdir)
    if "MYSQL_UNIX_PORT" in os.environ:
        monkeypatch.setenv("ZEPHIR_DB_SOCKET", os.environ["MYSQL_UNIX_PORT"])
    os.system("mysql --host=localhost --user=root  < {}/micro-db.sql".format(td_tmpdir))


@freeze_time("2019-02-18")
def test_create_bib_export_incr(td_tmpdir, env_setup, capsys, pytestconfig):
    for merge_version in ["v2", "v3"]:
        os.rename(
            os.path.join(td_tmpdir, "cache-{}-ref.db".format(merge_version)),
            os.path.join(
                td_tmpdir,
                "cache-{}-{}.db".format(
                    merge_version, datetime.datetime.today().strftime("%Y-%m-%d")
                ),
            ),
        )
        console = ConsoleMessenger(verbosity=pytestconfig.getoption("verbose"))
        ht_bib_incr(console=console, merge_version=merge_version, force=True)

        export_filename = "ht_bib_export_incr_{}.json".format(
            datetime.datetime.today().strftime("%Y-%m-%d")
        )

        assert filecmp.cmp(
            os.path.join(td_tmpdir, export_filename),
            os.path.join(
                td_tmpdir, "{}-ht_bib_export_incr_ref.json".format(merge_version)
            ),
        )
        # clean up to avoid name conflict next merge-version
        os.remove(os.path.join(td_tmpdir, export_filename))
