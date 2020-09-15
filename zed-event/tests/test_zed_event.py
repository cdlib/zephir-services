import json
import os

import jsonschema
import pytest

from zed.event import ZedEvent


def test_create_and_validate_zed_event(tmpdatadir):
    with open(os.path.join(tmpdatadir, "complete_event.json"), "r") as f:
        event_data = json.load(f)
    e = ZedEvent(event_data)
    assert json.dumps(event_data, sort_keys=True) == str(e)


def test_init_fields_set_to_default(tmpdatadir):
    with open(os.path.join(tmpdatadir, "missing_init_fields.json"), "r") as f:
        event_data = json.load(f)
    e = ZedEvent(event_data)
    assert e.data.get("timestamp", None) is not None
    assert e.data.get("process", None) is not None
    assert e.data.get("event", None) is not None

def test_create_event_details_by_argument(tmpdatadir):
    event_details = {"subject": "test_subject", "object": "test_object", "report": {"test": 1}}
    # create by setting instance details as properties
    ze2 = ZedEvent({}, subject=event_details["subject"],object=event_details["object"],report=event_details["report"])
    assert event_details == {k: ze2.data[k] for k in ze2.data.keys() & {"subject", "object", "report"}}

def test_create_event_details_by_property(tmpdatadir):
    event_details = {"subject": "test_subject", "object": "test_object", "report": {"test": 1}}
    # create by setting instance details as properties
    ze2 = ZedEvent()
    ze2.report = event_details["report"]
    assert ze2.report == event_details["report"]
    ze2.subject = event_details["subject"]
    assert ze2.subject == event_details["subject"]
    ze2.object = event_details["object"]
    assert ze2.object == event_details["object"]
    assert event_details == {k: ze2.data[k] for k in ze2.data.keys() & {"subject", "object", "report"}}

def test_validation(tmpdatadir):
    with pytest.raises(jsonschema.exceptions.ValidationError):
        invalid_event_data = {}
        ZedEvent.validate(invalid_event_data, schema="ZED")
    with open(os.path.join(tmpdatadir, "complete_event.json"), "r") as f:
        valid_event_data = json.load(f)
        assert ZedEvent.validate(valid_event_data, schema="ZED")
