import os
import sys
    
import pytest
import json

from cid_minter.zed_for_cid import CidZedEvent

@pytest.fixture
def setup_zed_msg_table(data_dir, tmpdir, scope="session"):
    msg_table = os.path.join(data_dir, "columns_prep.csv")
    zed_log = os.path.join(data_dir, "cid_zed.log")

    return {
        "msg_table": msg_table,
        "zed_log": zed_log
    }

def test_zed_for_cid(caplog, setup_zed_msg_table):
    msg_table = setup_zed_msg_table["msg_table"]
    zed_log = setup_zed_msg_table["zed_log"]

    zed_event = CidZedEvent(msg_table, zed_log)

    test_list = []
    test_item = {
        "msg_code": "pr0212", # assign new CID
        "event_data": {
            "process_key": "92b1be8b-5fa0-44e5-9796-0a2ab3a4d5f0",
            "msg_detail": "test msg details pr0212",
            "report": {"CID": "123456789", "RecordID": "100001", "Rd_seq_no": "1", "config_name": "test_config"},
            "subject": "test subject pr0212",
            "object": "test object pr0212",
        },
        "expected_event": {
            "status": {"type": "INFO", "zed_code": "200", "msg_code": "pr0212", "msg": "Assigned new CID", "details": "test msg details pr0212"}, 
            "action": "MintCID", 
            "type": "Preprun", 
            "subject": "test subject pr0212", 
            "object": "test object pr0212", 
            "topic": "Raw", 
            "process": "92b1be8b-5fa0-44e5-9796-0a2ab3a4d5f0", 
            "report": {"CID": "123456789", "RecordID": "100001", "Rd_seq_no": "1", "config_name": "test_config"}
        }
    }
    test_list.append(test_item)

    test_item = {
        "msg_code": "pr0213", # Assigned existing CID
        "event_data": {
            #"process_key": "92b1be8b-5fa0-44e5-9796-0a2ab3a4d5f0", same as previous event
            "msg_detail": "test msg details pr0213",
            "report": {"CID": "1234567890", "RecordID": "100002", "Rd_seq_no": "2", "config_name": "test_config"},
            "subject": "test subject pr0213",
            "object": "test object pr0213",
        },
        "expected_event": {
            "status": {"type": "INFO", "zed_code": "200", "msg_code": "pr0213", "msg": "Assigned existing CID", "details": "test msg details pr0213"},
            "action": "MintCID",
            "type": "Preprun",
            "subject": "test subject pr0213",
            "object": "test object pr0213",
            "topic": "Raw",
            "process": "92b1be8b-5fa0-44e5-9796-0a2ab3a4d5f0",
            "report": {"CID": "1234567890", "RecordID": "100002", "Rd_seq_no": "2", "config_name": "test_config"}
        }
    }
    test_list.append(test_item)

    for item in test_list:
        zed_event.merge_zed_event_data(item.get("event_data"))
        zed_event.create_zed_event(item.get("msg_code"))
        created_event = zed_event.zed_event
        created_event.pop("timestamp", None)
        created_event.pop("event", None)
        assert len(created_event) == len(item.get("expected_event"))
        assert created_event == item.get("expected_event")

    zed_event.close()

    # verify envets were saved in the log file
    with open(zed_log) as f:
        i = 0
        for line in f:
            created_event = json.loads(line.rstrip())
            created_event.pop("timestamp", None)
            created_event.pop("event", None)
            assert created_event == test_list[i].get("expected_event")
            i += 1

