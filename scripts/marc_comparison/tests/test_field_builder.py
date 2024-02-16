import os
import sys

import pytest
from field_builder import main, parse_pattern, process_file

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
