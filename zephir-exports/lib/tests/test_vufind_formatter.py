import json
import os
import shutil
import sys

import pytest

from vufind_formatter import VufindFormatter


def test_vufind_successfully_merged(td_tmpdir):
    records = []
    with open(os.path.join(td_tmpdir, "000000001.json"), "r") as f:
        for line in f:
            records.append(line)

    vufind_record = VufindFormatter.create_record("000000001", records)
    assert len(vufind_record.get_fields("974")) == 5
    assert len(vufind_record.get_fields("035")) == 7
    assert vufind_record.get_fields("974")[0]["u"] == "mdp.39015018415946"


def test_value_error_raised_with_empty_list():
    records = []
    with pytest.raises(ValueError):
        VufindFormatter.create_record("000000001", records)


def test_insert_subfield_inserts_correctly():
    subfields = []
    VufindFormatter._insert_subfield("b", "b_value", subfields)
    assert subfields == ["b", "b_value"]
    VufindFormatter._insert_subfield("z", "z_value", subfields)
    assert subfields == ["b", "b_value", "z", "z_value"]
    VufindFormatter._insert_subfield("c", "c_value", subfields)
    assert subfields == ["b", "b_value", "c", "c_value", "z", "z_value"]
    VufindFormatter._insert_subfield("g", "g_value", subfields)
    assert subfields == ["b", "b_value", "c", "c_value", "g", "g_value", "z", "z_value"]
    VufindFormatter._insert_subfield("zz", "zz_value", subfields)
    assert subfields == [
        "b",
        "b_value",
        "c",
        "c_value",
        "g",
        "g_value",
        "z",
        "z_value",
        "zz",
        "zz_value",
    ]
    VufindFormatter._insert_subfield("8", "8_value", subfields)
    assert subfields == [
        "8",
        "8_value",
        "b",
        "b_value",
        "c",
        "c_value",
        "g",
        "g_value",
        "z",
        "z_value",
        "zz",
        "zz_value",
    ]
