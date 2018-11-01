import os
import shutil
import sys

import pytest

from export_cache import ExportCache
from export_cache import CacheComparison


def test_loaded_false_when_cache_table_does_not_exists(td_tmpdir):
    does_no_exist = ExportCache(td_tmpdir, "does-not-exist")
    assert does_no_exist.cache_schema_exists_on_load == False


def test_loaded_true_when_cache_table_does_exists(td_tmpdir):
    exists = ExportCache(td_tmpdir, "empty-cache")
    assert exists.cache_schema_exists_on_load == True


def test_add_to_cache(td_tmpdir):
    cache = ExportCache(td_tmpdir, "empty-cache")
    assert cache.size() == 0
    new_entry = {
        "cache_id": "012345",
        "cache_key": "C7EE1838",
        "cache_data": '{"leader":"01158nam a22003491  4500"}',
        "cache_date": "2016-06-29 11:09:04",
    }
    cache.add(**new_entry)
    assert cache.size() == 1


def test_update_to_cache_when_all_new(td_tmpdir):
    cache = ExportCache(td_tmpdir, "empty-cache")
    first_entry = {
        "cache_id": "012345",
        "cache_key": "C7EE1838",
        "cache_data": '{"leader":"this is marc data"}',
        "cache_date": "2016-06-29 11:09:04",
    }
    cache.add(**first_entry)
    first_result = cache.get("012345")
    second_entry = {
        "cache_id": "012345",
        "cache_key": "NEWKEY",
        "cache_data": '{"leader":"this is new and will not match original"}',
        "cache_date": "NEWDATE",
    }
    cache.update(**second_entry)
    assert cache.get("012345")['cache_key'] == "NEWKEY"
    assert cache.get("012345")['cache_date'] == "NEWDATE"
    assert cache.get("012345")['data_date'] != first_result["data_date"]
    assert cache.get("012345")['data_key'] != first_result["data_key"]
    assert cache.get("012345")['cache_data'] != first_result["cache_data"]

def test_update_to_cache_key_only_without_data_update(td_tmpdir):
    cache = ExportCache(td_tmpdir, "empty-cache")
    first_entry = {
        "cache_id": "012345",
        "cache_key": "C7EE1838",
        "cache_data": '{"leader":"this is marc data"}',
        "cache_date": "2016-06-29 11:09:04",
    }
    cache.add(**first_entry)
    first_result = cache.get("012345")
    second_entry = {
        "cache_id": "012345",
        "cache_key": "NEWKEY",
        "cache_data": '{"leader":"this is marc data"}',
        "cache_date": "NEWDATE",
    }
    cache.update(**second_entry)
    assert cache.get("012345")['cache_key'] == "NEWKEY"
    assert cache.get("012345")['cache_date'] == "NEWDATE"
    assert cache.get("012345")['data_date'] == first_result["data_date"]
    assert cache.get("012345")['data_key'] == first_result["data_key"]
    assert cache.get("012345")['cache_data'] == first_result["cache_data"]

def test_remove_set(td_tmpdir):
    cache = ExportCache(td_tmpdir, "empty-cache")
    first_entry = {
        "cache_id": "012345",
        "cache_key": "C7EE1838",
        "cache_data": '{"leader":"01158nam a22003491  4500"}',
        "cache_date": "2016-06-29 11:09:04",
    }
    second_entry = {
        "cache_id": "67891",
        "cache_key": "R9PE2815",
        "cache_data": '{"leader":"02258nam a22003491  4500"}',
        "cache_date": "2018-06-29 11:09:04",
    }
    third_entry = {
        "cache_id": "11111",
        "cache_key": "2222222",
        "cache_data": '{"follower":"02258nam a22003491  4500"}',
        "cache_date": "2017-06-29 11:09:04",
    }
    cache.add(**first_entry)
    cache.add(**second_entry)
    cache.add(**third_entry)
    assert cache.size() == 3
    cache.remove_set(["012345", "67891"])
    assert cache.size() == 1


def test_cache_comparison(td_tmpdir):
    compare_index = {
        "uncached": "not-cached",
        "cached": "same-comparison-cache",
        "stale": "different-than-cache",
    }
    cache_index = {
        "unexamined": "not-in-comparison",
        "cached": "same-comparison-cache",
        "stale": "different-than-comparison",
    }
    comparison = CacheComparison(cache_index, compare_index)
    assert "uncached" in comparison.uncached
    assert len(comparison.uncached) == 1
    assert "cached" in comparison.verified
    assert len(comparison.verified) == 1
    assert "stale" in comparison.stale
    assert len(comparison.stale) == 1
    assert "unexamined" in comparison.unexamined
    assert len(comparison.unexamined) == 1
