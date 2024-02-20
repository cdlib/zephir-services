import os
import sys

import pytest
import pymarc
from field_builder import main, parse_pattern, update_value_at_location

def test_parse_pattern():
    with pytest.raises(ValueError) as err:
        parse_pattern("001=")
    assert str(err.value) == "Invalid pattern"

    with pytest.raises(ValueError) as err:
        parse_pattern("001={245$a$b}")
    assert str(err.value) == "Invalid pattern"

    with pytest.raises(ValueError) as err:
        parse_pattern("8={245$a}")
    assert str(err.value) == "Invalid pattern"

    with pytest.raises(ValueError) as err:
        parse_pattern("")
    assert str(err.value) == "Invalid pattern"

    assert parse_pattern("001=hvd.{245$a}") == (["245$a"], "001", "hvd.{}")
    assert parse_pattern("245$a={245$a} / {245$b} / {245$c}") == (["245$a", "245$b", "245$c"], "245$a", "{} / {} / {}")
    assert parse_pattern("245$a=245$a: {245$a}") == (["245$a"], "245$a", "245$a: {}")

def test_update_value_at_location(td_tmpdir):
    record = pymarc.parse_xml_to_array(os.path.join(td_tmpdir, "first.xml"))[0]

    with pytest.raises(ValueError) as err:
        update_value_at_location(record, "001$", "test")
    assert str(err.value) == "Invalid location"

    with pytest.raises(ValueError) as err:
        update_value_at_location(record, "", "test")
    assert str(err.value) == "Invalid location"

    with pytest.raises(ValueError) as err:
        update_value_at_location(record, "245", "test")
    assert str(err.value) == "Invalid control field location"

    r = update_value_at_location(record, "245$a", "test")
    assert r["245"]["a"] == "test"

    r = update_value_at_location(record, "035$a", "test")
    assert r["035"]["a"] == "test"

    r = update_value_at_location(record, "260$b", "test")
    assert r["260"]["b"] == "test"
    
