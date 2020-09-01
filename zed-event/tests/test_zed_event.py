import json
import os

import jsonschema
import pytest

from zed.event import ZedEvent


def test_create_and_validate_zed_event(tmpdatadir):
    with open(os.path.join(tmpdatadir, "complete_event.json"), "r") as f:
        event_data = json.load(f)
    e = ZedEvent(event_data)
    assert json.dumps(event_data) == str(e)


def test_init_fields_set_to_default(tmpdatadir):
    with open(os.path.join(tmpdatadir, "missing_init_fields.json"), "r") as f:
        event_data = json.load(f)
    e = ZedEvent(event_data)
    assert e.data.get("timestamp", None) is not None
    assert e.data.get("process", None) is not None
    assert e.data.get("event", None) is not None


def test_validation(tmpdatadir):
    with pytest.raises(jsonschema.exceptions.ValidationError):
        invalid_event_data = {}
        ZedEvent.validate(invalid_event_data, schema="ZED")
    with open(os.path.join(tmpdatadir, "complete_event.json"), "r") as f:
        valid_event_data = json.load(f)
        assert ZedEvent.validate(valid_event_data, schema="ZED")

# def test_use_of_zed_event_in_logging:
# 			# "type": "object",
# 			# "properties": {
# 			# 	"type": {
# 			# 		"type": "string"
# 			# 	},
# 			# 	"zed_code": {
# 			# 		"type": "string"
# 			# 	},
# 			# 	"msg_code": {
# 			# 		"type": "string"
# 			# 	},
# 			# 	"msg": {
# 			# 		"type": "string"
# 			# 	},
# 			# 	"details": {
# 			# 		"type": "string"
# 			# 	}
#     process_info = {
#         "topic":"raw",
#         "type":"test",
#         "subject":"test-run",
#         "object":"test-case"
#         "status"}
