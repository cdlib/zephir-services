import os
import json
import shutil
import sys

import plyvel
import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), 'helpers'))
<<<<<<< HEAD
=======
import utils
>>>>>>> 556eed7f17ac8d725dd4fd1f819a3e88df0a984f

@pytest.fixture
def tmpdatadir(request, tmpdir):
    """Copy test data into the temporary directory for tests.

    Note:
        1) Test data is located by convention. Create a directory with the same name as
        the test file (test-file: test_mymodule.py, test-data: test_mymodule/* )
        2) Using the temporary directory prevents overwriting of data in test suite

    Args:
        request: Fixture with test case filename, used to find test data.
        tmpdir: Fixture for test case temporary directory generated by pytest.

    Returns:
        Filepath of test data subdirectory within the temporary dirctory.

    """
<<<<<<< HEAD
    data_dirname = os.path.splitext(os.path.basename(request.fspath))[0]
    data_path = os.path.splitext(request.fspath)[0]
    tmp_data_path = os.path.join(tmpdir, data_dirname)
    shutil.copytree(data_path, tmp_data_path)
    return tmp_data_path


@pytest.fixture
def json_loader(tmpdatadir, request):
    """Load json data files into a Dict

    Args:
        tmpdatadir: Populated temporary directory with test data

    Returns:
        Json data files converted into dictionary

    """
    files = [f for f in os.listdir(tmpdatadir) if os.path.isfile(os.path.join(tmpdatadir,f)) and f.endswith(".json")]
    data = {}
    for file in files:
        with open(os.path.join(tmpdatadir,file), "r") as read_file:
            data[file] = json.load(read_file)
    return data
=======
    td_dirname = os.path.splitext(os.path.basename(request.fspath))[0]
    td_path = os.path.splitext(request.fspath)[0]
    tmp_td_path = os.path.join(tmpdir, td_dirname)
    shutil.copytree(td_path, tmp_td_path)
    return tmp_td_path


@pytest.fixture
def json_loader(td_tmpdir, request):
    """Create a case data set by loading json test data

    Note:
        1) This expects test_cases.json file in the test directory

    Args:
        td_tmpdir: Fixture for test case temporary directory generated by pytest
        and populated with test data

    Returns:
        Json test case data converted into dictionary

    """
    files = [f for f in os.listdir(td_tmpdir) if os.path.isfile(os.path.join(td_tmpdir,f)) and f.endswith(".json")]
    data = {}
    for file in files:
        with open(os.path.join(td_tmpdir,file), "r") as read_file:
            data[file] = json.load(read_file)
    return data


@pytest.fixture
def prepare_primary_db_tests(td_tmpdir, request, json_loader):
    """
    Note:
        Compound method to create database and test case data for assertions
    Args:
        td_tmpdir: Test case temporary directory generated by pytest
        and populated with test data
        request: Fixture with request parameters from test case (for json file)
        json_loader: Load json file into python structure

    Returns:
        Test case data and database path

    """
    data = json_loader[request.param]
    db_path = utils.create_primary_ldb(td_tmpdir, data)
    cases = utils.split_cases(data)
    return {"cases":cases, "db_path":db_path}
>>>>>>> 556eed7f17ac8d725dd4fd1f819a3e88df0a984f


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
