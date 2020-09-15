import os

import pytest

from zed.table import ZedTable
from zed.event import ZedEvent
from zed.error import *

def test_create_validated_event_with_table(tmpdatadir):
    zt = ZedTable(os.path.join(tmpdatadir,'package_zed.csv'), delimiter="\t")
    ze1 = ZedEvent(zt.get("ze0001"))
    assert ze1.data.get("status").get("zed_code") == "200"
    assert ZedEvent.validate(ze1.data)

def test_empty_table(tmpdatadir):
    zt = ZedTable()
    with pytest.raises(ZedEventNotFoundError) as excinfo:
        zt.get("ze0001")
    assert "ze0001" in str(excinfo.value)

def test_missing_event(tmpdatadir):
    zt = ZedTable(os.path.join(tmpdatadir,'package_zed.csv'), delimiter="\t")
    with pytest.raises(ZedEventNotFoundError) as excinfo:
        zt.get("ez0001")
    assert "ez0001" in str(excinfo.value)
