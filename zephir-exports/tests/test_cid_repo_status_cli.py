import os
import sys

import pytest

from cid_repo_status_cli import cid_repo_status_cli

@pytest.fixture
def env_setup(td_tmpdir, monkeypatch):
    # use tmpdir configuration
    monkeypatch.setenv(
        "ZEPHIR_OVERRIDE_CONFIG_PATH", os.path.join(str(td_tmpdir), "config")
    )

    if "MYSQL_UNIX_PORT" in os.environ:
        monkeypatch.setenv("ZEPHIR_DB_SOCKET", os.environ["MYSQL_UNIX_PORT"])
    # load test data
    os.system("mysql --host=localhost --user=root  < {}/micro-db.sql".format(td_tmpdir))


@pytest.mark.parametrize("test_case", ["all", "present", "absent"])
def test_cid_repo_status_params(test_case, env_setup, td_tmpdir, capsys, pytestconfig):
    with pytest.raises(SystemExit) as pytest_e:
        sys.argv = [
            "",
            os.path.join(td_tmpdir, "cid.change.txt"),
            "--status",
            test_case,
            "--output-filepath",
            os.path.join(str(td_tmpdir),"{}_output_generated.txt".format(test_case)),
            "--verbosity",
            pytestconfig.getoption("verbose"),
        ]
        cid_repo_status_cli()
    stdout, stderr = capsys.readouterr()
    generated_file = open(os.path.join(str(td_tmpdir),"{}_output_generated.txt".format(test_case)),mode='r').read()
    file_testcase = open(os.path.join(str(td_tmpdir),"{}_output.txt".format(test_case)),mode='r').read()
    assert stdout == file_testcase
    assert generated_file == file_testcase
