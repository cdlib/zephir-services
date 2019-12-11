import json
import os

import jsonschema
import pytest

from zed_event import ZedEvent


def test_create_zed_event(td_tmpdir):
    with open(os.path.join(td_tmpdir, "complete_event.json"), "r") as f:
        event_dict = json.load(f)
    e = ZedEvent(event_dict)
    assert e.isvalidated() is True
    assert json.dumps(event_dict) == str(e)


def test_init_fields_default(td_tmpdir):
    with open(os.path.join(td_tmpdir, "missing_init_fields.json"), "r") as f:
        event_dict = json.load(f)
    e = ZedEvent(event_dict, validate=False)
    assert e.event().get("timestamp", None) is not None
    assert e.event().get("process", None) is not None
    assert e.event().get("event", None) is not None


def test_do_not_init_fields(td_tmpdir):
    with open(os.path.join(td_tmpdir, "missing_init_fields.json"), "r") as f:
        event_dict = json.load(f)
    e = ZedEvent(event_dict, init=None, validate=False)
    assert e.event().get("timestamp", None) is None
    assert e.event().get("process", None) is None
    assert e.event().get("event", None) is None


def test_skip_validation_errors():
    event_dict = {}
    e = ZedEvent(event_dict, init=None, validate=False)
    assert e.isvalidated() is False


def test_validation_errors_default(td_tmpdir):
    event_dict = {}
    with pytest.raises(jsonschema.exceptions.ValidationError):
        e = ZedEvent(event_dict)
        assert e.isvalidated() is False
