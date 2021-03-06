import os
import shutil

import pytest


@pytest.fixture
def td_tmpdir(request, tmpdir, monkeypatch):
    """Copy test data into the temporary directory for tests.

    Note:
        1) Test data is located by convention. Create a directory with the same name as
        the test file (test-file: test_mymodule.py, test-data: test_mymodule/* )

    Args:
        request: Fixture with test case filename, used to find test data.
        tmpdir: Fixture for test case temporary directory generated by pytest.

    Returns:
        Filepath of test data subdirectory within the temporary dirctory.

    """
    td_dirname = os.path.splitext(os.path.basename(request.fspath))[0]
    td_path = os.path.join(os.path.dirname(__file__), td_dirname)
    tmp_td_path = os.path.join(tmpdir, td_dirname)
    shutil.copytree(td_path, tmp_td_path)
    return tmp_td_path


def pytest_configure(config):
    if os.environ.get("ZEPHIR_ENV"):
        os.environ["ZEPHIR_TEST_SWAP_ENV"] = os.environ.get("ZEPHIR_ENV")
    os.environ["ZEPHIR_ENV"] = "test"


def pytest_unconfigure(config):
    if not os.environ.get("ZEPHIR_TEST_SWAP_ENV"):
        del os.environ["ZEPHIR_ENV"]
