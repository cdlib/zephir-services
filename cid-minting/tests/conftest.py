import os

import pytest


@pytest.fixture
def data_dir(request):
    """
    Note:
        Test data is located by convention in a directory with the same name as
        the test file, for example
        test-file: test_mymodule.py
        data-dir: test_mymodule

    Args:
        request: Fixture with test case filename, used to find test data directory.

    Returns:
        Filepath of the default test data directory.

    """
    request_path = os.path.dirname(request.fspath)
    request_filename = os.path.basename(request.fspath)
    td_dirname = os.path.splitext(request_filename)[0]
    return os.path.join(request_path, td_dirname)


