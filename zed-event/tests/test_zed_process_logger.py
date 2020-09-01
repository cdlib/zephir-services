import os

import pandas as pd

from zed.table import ZedTable
from zed.event import ZedEvent
from zed.process_logger import ZedProcessLogger

def test_create_validated_event_with_table_factory(tmpdatadir):
    zt = ZedProcessLogger(os.path.join(tmpdatadir,'package_zed.csv'), delimiter="\t")
    ze1 = ZedEvent(zt.get("ze0001"))
    assert ze1._data.get("status").get("zed_code") == "200"
    print(ze1)
    process_data = {"process":ze1.process}
    ze2 = ZedEvent(zt.get("FAILURE_EVENT"), {"process": ze1.process})
    print(ze2)
