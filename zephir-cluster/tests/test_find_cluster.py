import os
import sys

import pytest

from find_cluster import main


@pytest.fixture
def env_setup(td_tmpdir, monkeypatch):
    monkeypatch.setenv(
        "ZED_OVERRIDE_CONFIG_PATH", os.path.join(str(td_tmpdir), "config")
    )
    os.system(
        "mysql --host=localhost --user=root  < {}/cid_related_tables.sql".format(
            td_tmpdir
        )
    )


def test_find_cluster_with_no_files(env_setup, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = [""]
        main()
    out, err = capsys.readouterr()
    assert "No files given to process." in err
    assert pytest_e.type, pytest_e.value.code == [SystemExit, 1]


def test_found_oclc_matches(env_setup, td_tmpdir, capsys):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = ["", os.path.join(td_tmpdir, "found_oclcs.csv")]
        main()
    out, err = capsys.readouterr()
    assert "8727632" in err
    assert "002492721" in err
    assert pytest_e.type, pytest_e.value.code == [SystemExit, 0]
