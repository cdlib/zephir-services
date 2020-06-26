import pytest

from oclc_lookup import get_primary_ocn


def test_get_primary_ocn(split_cases, primary_ldb_path):
    results = [get_primary_ocn(ocn, primary_ldb_path) for ocn in split_cases["input"]]
    assert split_cases["expected"].sort() == results.sort()
