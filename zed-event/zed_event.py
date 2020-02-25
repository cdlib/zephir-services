# from datetime import datetime, timezone
# import json
# import re
# import os
# import uuid
#
# import jsonschema
#
#
# class ZedEvent:
#     """For creating and validating Zed Event JSON strings for logs and other services.
#
#         Usage examples:
#
#         isvalid_zed = ZedEvent.validate(zed_data, schema="SCHEMA_NAME")
#
#         zed_event = ZedEvent(event_data)
#
#         validated_zed_event = ZedEvent(event_data, validate="SCHEMA_NAME")
#     """
#
#     # CLASS-LEVEL BLOCK
#     # Define schemas and validation methods for Zed Events.
#     __schemas_dir = os.path.join(os.path.dirname(__file__), "schemas")
#     __schemas = {}
#     for filename in os.listdir(os.path.join(os.path.dirname(__file__), "schemas")):
#         if filename.endswith(".json"):
#             with open(os.path.join(__schemas_dir, filename), "r") as f:
#                 schema_data = f.read()
#                 schema = json.loads(schema_data)
#                 __schemas[re.search("^(.*)_schema", filename).group(1).upper()] = schema
#
#     @classmethod
#     def schemas(cls,):
#         """Return a dictionary of available json schemas.
#
#             Returns: dict of schemas
#         """
#         return cls.__schemas
#
#     @classmethod
#     def validate(cls, data, schema="ZED"):
#         """Validate Zed Event data against a json schema."""
#         jsonschema.validate(data, ZedEvent.schemas()[schema])
#         return True
#
#     @classmethod
#     def generate_key(cls):
#         """Generate a UUID version 4 key."""
#         return str(uuid.uuid4())
#
#     @classmethod
#     def generate_timestamp(cls):
#         """Generate a UTC ISO 8601 formatted timestamp"""
#         return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
#
#     # INSTANCE-LEVEL BLOCK
#     def __init__(self, data, init=["event", "process", "timestamp"], validate=None):
#         """ZedEvent instance for creating ZedEvent objects for logging.
#
#         Args:
#             data (dict): The Zed Event properties as a dictionary.
#             init (:list:`str`, optional): Zed Event properties to be initialized
#                 on object creation if they are not passed in the data. Default:
#                 event, process, and timestamp.
#             validate (str, optional): Class-defined schema to validate against
#                 object. Default: None.
#         """
#         self._data = data
#         self._validated = False
#
#         if init is None:
#             init = []
#         if "event" in init and ("event" not in self._data or not self._data["event"]):
#             self._data["event"] = ZedEvent.generate_key()
#         if "process" in init and (
#             "process" not in self._data or not self._data["process"]
#         ):
#             self._data["process"] = ZedEvent.generate_key()
#         if "timestamp" in init and (
#             "timestamp" not in self._data or not self._data["timestamp"]
#         ):
#             self._data["timestamp"] = ZedEvent.generate_timestamp()
#
#         if validate:
#             self._validated = ZedEvent.validate(self._data, validate)
#
#     @property
#     def data(self):
#         """dict: The Zed Event data for the object"""
#         return self._data
#
#     def isvalidated(self):
#         """Return whether or not an object has been validated against a schema
#         Returns:
#         True if object has been validated, False otherwise"""
#         return self._validated
#
#     def __str__(self):
#         """Print object represented as Zed Event data in json"""
#         return json.dumps(self._data)
