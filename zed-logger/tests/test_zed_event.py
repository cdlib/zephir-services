import json
import os

import pytest

from zed_event import ZedEvent


def test_create_zed_event(td_tmpdir):
    with open(os.path.join(td_tmpdir, "complete_event.json"), "r") as f:
        event_json = json.load(f)
    e = ZedEvent(event_json)
    assert json.dumps(event_json) == str(e)


def test_create_zed_event(td_tmpdir):
    with open(os.path.join(td_tmpdir, "missing_timestamp.json"), "r") as f:
        event_json = json.load(f)
    e = ZedEvent(event_json)
    assert json.dumps(event_json) == str(e)
