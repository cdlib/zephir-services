import os

import pytest

from zephir_cluster import ZephirCluster


@pytest.fixture
def env_setup(td_tmpdir, monkeypatch):
    monkeypatch.setenv(
        "ZEPHIR_OVERRIDE_CONFIG_PATH", os.path.join(str(td_tmpdir), "config")
    )
    monkeypatch.setenv("ZEPHIR_CACHE_PATH", os.path.join(str(td_tmpdir)))
    if "MYSQL_UNIX_PORT" in os.environ:
        monkeypatch.setenv("ZEPHIR_DB_SOCKET", os.environ["MYSQL_UNIX_PORT"])
    os.system(
        "mysql --host=localhost --user=root  < {}/cid_related_tables.sql".format(
            td_tmpdir
        )
    )


def test_find_cid_candidate_by_oclc_success(env_setup):
    candidate_list = ZephirCluster.find_candidate_clusters(oclcs=["1570562"])
    assert "006191924" in candidate_list
