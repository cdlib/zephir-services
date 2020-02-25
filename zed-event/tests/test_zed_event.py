# import json
# import os
#
# import jsonschema
# import pytest
#
# from zed_event import ZedEvent
#
#
# def test_create_and_validate_zed_event(td_tmpdir):
#     with open(os.path.join(td_tmpdir, "complete_event.json"), "r") as f:
#         event_data = json.load(f)
#     e = ZedEvent(event_data, validate="ZED")
#     assert e.isvalidated() is True
#     assert json.dumps(event_data) == str(e)
#
#
# def test_init_fields_default(td_tmpdir):
#     with open(os.path.join(td_tmpdir, "missing_init_fields.json"), "r") as f:
#         event_data = json.load(f)
#     e = ZedEvent(event_data)
#     assert e.data.get("timestamp", None) is not None
#     assert e.data.get("process", None) is not None
#     assert e.data.get("event", None) is not None
#
#
# def test_do_not_init_fields(td_tmpdir):
#     with open(os.path.join(td_tmpdir, "missing_init_fields.json"), "r") as f:
#         event_data = json.load(f)
#     e = ZedEvent(event_data, init=None)
#     assert e.data.get("timestamp", None) is None
#     assert e.data.get("process", None) is None
#     assert e.data.get("event", None) is None
#
#
# def test_skip_validation():
#     event_data = {}
#     e = ZedEvent(event_data, init=None)
#     assert e.isvalidated() is False
#
#
# def test_validation_errors():
#     event_data = {}
#     with pytest.raises(jsonschema.exceptions.ValidationError):
#         ZedEvent(event_data, validate="ZED")
