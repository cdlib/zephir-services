import os

import pytest

from lib.vufind_formatter import VufindFormatter


def test_vufind_merged(td_tmpdir, request):
    records = []
    with open(os.path.join(td_tmpdir, "000000001.json"), "r") as f:
        for line in f:
            records.append(line)

    vufind_record = VufindFormatter.create_record("000000001", records)

    zero35s = []
    zero35s_expected = ['(MiU)000000001',
    'sdr-miu000000001',
    'sdr-TEST-SECOND-SDR',
    'sdr-TEST-THIRD-SDR',
    '(OCoLC)10000001',
    '(OCoLC)20000022',
    '(OCoLC)30000333',
    '(CaOTULAS)159818001',
    '(RLIN)MIUGMIUGPRIB-B']
    for zero35 in vufind_record.get_fields("035"):
        zero35s.append(zero35["a"])
    assert zero35s == zero35s_expected

    assert len(vufind_record.get_fields("974")) == 5
    assert len(vufind_record.get_fields("035")) == 9
    assert vufind_record.get_fields("974")[0]["u"] == "mdp.39015018415946"

def test_vufind_merged_no_ocn_in_base(td_tmpdir):
    # the base record has no ocns, but the holdings do have ocns
    records = []
    with open(os.path.join(td_tmpdir, "000000002.json"), "r") as f:
        for line in f:
            records.append(line)

    vufind_record = VufindFormatter.create_record("000000002", records)

    zero35s = []
    zero35s_expected = ['(MiU)000000002',
    'sdr-miu000000002',
    'sdr-TEST-SECOND-SDR',
    'sdr-TEST-THIRD-SDR',
    '(OCoLC)10000001',
    '(OCoLC)20000022',
    '(OCoLC)30000333',
    '(CaOTULAS)159818001',
    '(RLIN)MIUGMIUGPRIB-B']
    for zero35 in vufind_record.get_fields("035"):
        zero35s.append(zero35["a"])
    assert zero35s == zero35s_expected

    assert len(vufind_record.get_fields("974")) == 5
    assert len(vufind_record.get_fields("035")) == 9
    assert vufind_record.get_fields("974")[0]["u"] == "mdp.39015018415946"

def test_vufind_merged_no_ocns(td_tmpdir):
    # no records has ocns
    records = []
    with open(os.path.join(td_tmpdir, "000000003.json"), "r") as f:
        for line in f:
            records.append(line)

    vufind_record = VufindFormatter.create_record("000000003", records)

    zero35s = []
    zero35s_expected = ['(MiU)000000003',
    'sdr-miu000000003',
    'sdr-TEST-SECOND-SDR',
    'sdr-TEST-THIRD-SDR',
    '(CaOTULAS)159818001',
    '(RLIN)MIUGMIUGPRIB-B']
    for zero35 in vufind_record.get_fields("035"):
        zero35s.append(zero35["a"])
    assert zero35s == zero35s_expected

    assert len(vufind_record.get_fields("974")) == 5
    assert len(vufind_record.get_fields("035")) == 6
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
