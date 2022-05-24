import os
import pytest

from my_app import main

def test_my_function(capsys):
    with pytest.raises(SystemExit) as pytest_e:
        main()
    out, err = capsys.readouterr()
    assert "testing app" in out
    assert [pytest_e.type, pytest_e.value.code] == [SystemExit, 0]
