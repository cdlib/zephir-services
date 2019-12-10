from datetime import datetime, timezone, timedelta
import json
import re
import os
import uuid

import jsonschema


# (datetime.now(timezone.utc) + timedelta(days=3)).timestamp() * 1e3


class ZedEvent:
    """ Zed Event """

    # Load ZED schemas for validation
    __schemas_dir = os.path.join(os.path.dirname(__file__), "schemas")
    __schemas = {}
    for filename in os.listdir(os.path.join(os.path.dirname(__file__), "schemas")):
        if filename.endswith(".json"):
            with open(os.path.join(__schemas_dir, filename), "r") as f:
                schema_data = f.read()
                schema = json.loads(schema_data)
                __schemas[re.search("^(.*)_schema", filename).group(1).upper()] = schema

    def __init__(
        self,
        event={},
        init=["event", "process", "timestamp"],
        validate=True,
        schema="ZED",
    ):
        self._event = event
        self._is_validated = False

        if init is None:
            init = []
        if "event" in init and ("event" not in self._event or not self._event["event"]):
            self._event["event"] = str(uuid.uuid4())
        if "process" in init and (
            "process" not in self._event or not self._event["process"]
        ):
            self._event["process"] = str(uuid.uuid4())
        if "timestamp" in init and (
            "timestamp" not in self._event or not self._event["timestamp"]
        ):
            self._event["timestamp"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            )

        if validate:
            ZedEvent.validate(self._event, schema)

    def __str__(self):
        print(json.dumps(self._event))
        return json.dumps(self._event)

    @classmethod
    def validate(self, event, schema="ZED"):
        jsonschema.validate(event, ZedEvent.__schemas[schema])
        self.validated = True
