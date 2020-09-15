from datetime import datetime, timezone
import json
import re
import os
import uuid

import jsonschema


class ZedEvent:
    """For creating and validating Zed Event JSON strings for logs and other services.

        Usage examples:

        isvalid_zed = ZedEvent.validate(zed_data, schema="SCHEMA_NAME")

        zed_event = ZedEvent(event_data)

        validated_zed_event = ZedEvent(event_data, validate="SCHEMA_NAME")
    """

    # CLASS-LEVEL BLOCK
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
    def schemas(cls,):
        """
            Returns: JSOM Schemas (dict)
        """
        return cls.__schemas

    @classmethod
    def validate(cls, data, schema="ZED"):
        """
            Validate Zed Event data against a json schema
            Args:
                data: ZED Data (dict)
                schema: JSON Schema (jsonschema)
                Returns: Validation success/failure (bool)
        """
        jsonschema.validate(data, ZedEvent.schemas()[schema])
        return True

    @classmethod
    def generate_id(cls):
        """
            Returns: Generate a UUID version 4 ID
        """
        return str(uuid.uuid4())

    @classmethod
    def generate_timestamp(cls):
        """
            Generate a UTC ISO 8601 formatted timestamp
        """
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # INSTANCE-LEVEL BLOCK
    def __init__(self, data={}, object=None, process=None, subject=None, report=None):
        """
            Zed Event instance for creating ZedEvent objects for logging.

            Args:
                data (dict): The Zed Event data as a dictionary.
        """
        self.__data = data

        # set common event-instance values by argument
        if object:
            self.object = object
        if process:
            self.process = process
        if report:
            self.report = report
        if subject:
            self.subject = subject

        # generate values if not supplied
        if "event" not in self.__data or not self.__data["event"]:
            self.__data["event"] = ZedEvent.generate_id()
        if "process" not in self.__data or not self.__data["process"]:
            self.__data["process"] = process or ZedEvent.generate_id()
        if "timestamp" not in self.__data or not self.__data["timestamp"]:
            self.__data["timestamp"] = ZedEvent.generate_timestamp()


    @property
    def data(self):
        """
            Returns: Zed Event data (dict)
        """
        return self.__data

    @property
    def id(self):
        """
            Returns: Event GUID (str)
        """
        return self.__data["event"]

    @property
    def process(self):
        """
            Returns: Process GUID (str)
        """
        return self.__data["process"]

    @process.setter
    def process(self, value):
        self.__data["process"]=value

    @property
    def subject(self):
        """
            Returns: ZED Event's Subject (str)
        """
        return self.__data["subject"]

    @subject.setter
    def subject(self, value):
        self.__data["subject"]=value

    @property
    def object(self):
        """
            Returns: ZED Event's Object (str)
        """
        return self.__data["object"]

    @object.setter
    def object(self, value):
        self.__data["object"]=value

    @property
    def report(self):
        """
            Returns: ZED Event's Report (str)
        """
        return self.__data["report"]

    @report.setter
    def report(self, value):
        self.__data["report"]=value

    def __str__(self):
        """
            Returns: ZED Event data in JSON (str)
        """
        return json.dumps(self.__data, sort_keys=True)
