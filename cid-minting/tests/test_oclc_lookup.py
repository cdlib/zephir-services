import os

import pytest
import plyvel

from oclc_lookup import get_primary_ocn


@pytest.mark.parametrize("prepare_primary_db_tests", ["primary.json"], indirect=True)
def test_get_primary_ocn(td_tmpdir, prepare_primary_db_tests):
    cases = prepare_primary_db_tests["cases"]
    db_path = prepare_primary_db_tests["db_path"]
    results = [get_primary_ocn(ocn, db_path) for ocn in cases["input"]]
    assert cases["expected"].sort() == results.sort()
    assert True
