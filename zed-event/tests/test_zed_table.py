import os

import pandas as pd

from zed_table import ZedTable


def test_create_validated_event_with_table_factory(td_tmpdir):
    zed_table = pd.read_csv(os.path.join(td_tmpdir,'package_zed.csv'),dtype={'status_zed_code': object})
    zt = ZedTable(zed_table)
    ze = zt.create_event("TEST_MESSAGE_SUCCESS")
    #assert ze.isvalidated() is True



    # ze = zt.create_event("TEST_MESSAGE_SUCCESS", data={"status_msg":"This is an override of the message"}, validate="ZED")
