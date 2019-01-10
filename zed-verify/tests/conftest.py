import os
import shutil
import sys

import pytest


# Add tests to path
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))


@pytest.fixture
def data_setup(data_path, tmp_path):
    # for every test, copy test files over into the tmpdir
    for entry in os.scandir(data_path):
        if entry.is_file():
            shutil.copy(entry, os.path.join(tmp_path, entry.name))


def pytest_configure(config):
    os.environ["ZED_ENV"] = "test"
    tmpdir = os.path.join(os.path.dirname(__file__), "tmp")
    if not os.path.exists(tmpdir):
        os.makedirs(tmpdir)
    return config


def pytest_unconfigure(config):
    os.environ["ZED_ENV"] = ""
    tmpdir = os.path.join(os.path.dirname(__file__), "tmp")
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
    return config
