import os

import pandas as pd

from zed.table import ZedTable
from zed.event import ZedEvent
from zed.process import ZedProcess


def test_create_process():
    zp = ZedProcess({"type":"process_type"})
    ze1 = zp.create_event()
    assert ze1.process == zp.id

def test_create_event_with_process_defaults():
    zp = ZedProcess({"type":"process_type", "topic":"process_topic", "action":"process_action"})
    ze1 = zp.create_event({"action":"event_action"})
    assert ze1.process == zp.id
    assert ze1.data["type"] == "process_type"
    assert ze1.data["topic"] == "process_topic"
    assert ze1.data["action"] == "event_action"

def test_create_event_with_process_and_table(tmpdatadir):
    zt = ZedTable(os.path.join(tmpdatadir,'columns_prep.csv'))
    zp = ZedProcess()
    ze1 = zp.create_event(zt.get("pr0021"))
    assert ze1.process == zp.id
    assert ze1.data["status"]["msg_code"] == "pr0021"

def test_diff_events_share_process(tmpdatadir):
    zt = ZedTable(os.path.join(tmpdatadir,'columns_prep.csv'))
    zp = ZedProcess()
    ze1 = zp.create_event(zt.get("pr0021"))
    ze2 = zp.create_event(zt.get("pr0006"))
    assert ze1.process == ze2.process
    assert ze1.id != ze2.id

def test_create_event_details_by_param(tmpdatadir):
    zt = ZedTable(os.path.join(tmpdatadir,'columns_prep.csv'))
    zp = ZedProcess()
    details ={"subject": "test_subject", "object": "test_object", "report": {"test": 1}}
    # create by passing instance details at init
    zed_data = {**zt.get("ZED_ERROR_DUP_BARCD"), **details}
    ze1 = zp.create_event(zed_data)
    assert details == {k: ze1.data[k] for k in ze1.data.keys() & {"subject", "object", "report"}}

def test_create_event_details_by_property(tmpdatadir):
    zt = ZedTable(os.path.join(tmpdatadir,'columns_prep.csv'))
    zp = ZedProcess()
    event_details = {"subject": "test_subject", "object": "test_object", "report": {"test": 1}}
    # create by setting instance details as properties
    ze2 = zp.create_event(zt.get("ZED_ERROR_DUP_BARCD"))
    ze2.report = event_details["report"]
    ze2.subject = event_details["subject"]
    ze2.object = event_details["object"]
    assert event_details == {k: ze2.data[k] for k in ze2.data.keys() & {"subject", "object", "report"}}
