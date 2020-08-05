import os
import json
import shutil
import sys

import pandas
import plyvel
import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))


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
    data_dirname = os.path.splitext(os.path.basename(request.fspath))[0]
    data_path = os.path.splitext(request.fspath)[0]
    tmp_data_path = os.path.join(tmpdir, data_dirname)
    shutil.copytree(data_path, tmp_data_path)
    return tmp_data_path


@pytest.fixture
def csv_to_df_loader(tmpdatadir, request):
    """Load json data files into a Dict

    Args:
        tmpdatadir: Populated temporary directory with test data

    Returns:
        CSV data files converted into dictionary of pandas dataframes

    """
    files = [
        f
        for f in os.listdir(tmpdatadir)
        if os.path.isfile(os.path.join(tmpdatadir, f)) and f.endswith(".csv")
    ]
    data = {}

    for file in files:
        with open(os.path.join(tmpdatadir, file), "r") as read_file:
            data[file] = pandas.read_csv(read_file)
    return data