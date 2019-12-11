from datetime import datetime, timezone
import json
import re
import os
import uuid

import jsonschema


class ZedEvent:
    """ Zed Event """

    # CLASS BLOCK
    # Define schemas and validation methods for Zed Events.
    __schemas_dir = os.path.join(os.path.dirname(__file__), "schemas")
    __schemas = {}
    for filename in os.listdir(os.path.join(os.path.dirname(__file__), "schemas")):
        if filename.endswith(".json"):
            with open(os.path.join(__schemas_dir, filename), "r") as f:
                schema_data = f.read()
                schema = json.loads(schema_data)
                __schemas[re.search("^(.*)_schema", filename).group(1).upper()] = schema

    @classmethod
    def schemas(self, schema):
        return ZedEvent.__schemas[schema]

    @classmethod
    def validate(self, event, schema="ZED"):
        jsonschema.validate(event, ZedEvent.schemas(schema))
        return True

    @classmethod
    def generate_key(self):
        return str(uuid.uuid4())

    @classmethod
    def generate_timestamp(self):
        return datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )

    # INSTANCE BLOCK
    # Define ZedEvent instances for event logging
    def __init__(
        self,
        event={},
        init=["event", "process", "timestamp"],
        validate=True,
        schema="ZED",
    ):
        self._event = event
        self._validated = False

        if init is None:
            init = []
        if "event" in init and ("event" not in self._event or not self._event["event"]):
            self._event["event"] = ZedEvent.generate_key()
        if "process" in init and (
            "process" not in self._event or not self._event["process"]
        ):
            self._event["process"] = ZedEvent.generate_key()
        if "timestamp" in init and (
            "timestamp" not in self._event or not self._event["timestamp"]
        ):
            self._event["timestamp"] = ZedEvent.generate_timestamp()

        if validate:
            self._validated = ZedEvent.validate(self._event, schema)

    def event(self):
        return self._event

    def isvalidated(self):
        return self._validated

    def __str__(self):
        return json.dumps(self._event)
