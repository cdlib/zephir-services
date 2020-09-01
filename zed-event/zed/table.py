import json
import re

import numpy as np
import pandas as pd

from zed.event import ZedEvent

class ZedTable:
    def __init__(self, file, delimiter=","):
        self._table_df = pd.read_csv(file, delimiter=delimiter, dtype=str)
        self.table = self._table_df.values
        self.headers = list(self._table_df.columns)

    def create_event(self, code, data={}, init=["event", "process", "timestamp"], validate=None):
        coded_list = self.table[self.table[:,1]==code][0]
        coded_defaults = self.flat_to_dict(dict(zip(self.header, coded_list))) #dict(zip(self.header, coded_list))
        coded_event = ZedEvent({**coded_defaults, **data}, init, validate)
        return coded_event

    def get(self, key):
        events = self._table_df.loc[self._table_df["status_msg_code"] == key]
        if events.empty and "status_shorthand" in self.headers:
            # check shortand if not found by status_msg_code
            events = self._table_df.loc[self._table_df["status_shorthand"] == key]
        if not events.empty:
            return self._unflatten(events.iloc[0].to_dict())

    def _unflatten(self, flat_event):
        event_dict = {
            "event": flat_event.get("event"),
            "object": flat_event.get("object"),
            "process": flat_event.get("process"),
            "report": flat_event.get("report"),
            "status": {
                "msg": flat_event.get("status_msg"),
                "msg_code": flat_event.get("status_msg_code"),
                "type": flat_event.get("status_type"),
                "zed_code": flat_event.get("status_zed_code")
                },
            "subject": flat_event.get("subject"),
            "timestamp": flat_event.get("timestamp"),
            "topic": flat_event.get("topics"),
            "type": flat_event.get("topic"),
            }
        return event_dict


#
# def nest_prefix(data, prefixes=[]):
#     new_dict = {}
#     for k, v in data.items():
#         for prefix in prefixes:
#             if re.match("^"+prefix+"_", k):
#                 result = nest_prefix({k.split(prefix+"_")[1]:v},prefixes)
#                 if prefix in new_dict:
#                     for k, v in result.items():
#                         new_dict[prefix][k] = v
#                 else:
#                     new_dict[prefix] = result
#             else:
#                 new_dict[k]=v
#     return new_dict
#
# def nest_dict(data, prefixes=[]):
#     new_dict = {}
#     for key in data:
#         for prefix in prefixes:
#             if re.match("^"+prefix+"_", key):
#                 result = nest_dict({key.split(prefix+"_")[1]:data[key]},prefixes)
#                 if prefix in new_dict:
#                     for key in result:
#                         new_dict[prefix][key] = result[key]
#                 else:
#                     new_dict[prefix] = result
#             else:
#                 new_dict[key]=data[key]
#     return new_dict
#
#     def nest_dict_compact(data, prefixes=[]):
#     new_dict = {}
#     for key in data:
#         match = re.match("^(\w+?)_", key)
#         if match:
#             prefix = match.group(1)
#             if prefix in prefixes:
#                 result = nest_dict_compact({key.split(prefix+"_")[1]:data[key]},prefixes)
#                 if prefix in new_dict:
#                     for key in result:
#                         new_dict[prefix][key] = result[key]
#                 else:
#                     new_dict[prefix] = result
#             else:
#                 new_dict[key]=data[key]
#     return new_dict
#
# {"event": "30be6cd1-02ba-47e9-be0d-988561aeafea", "object": "test",
# "process": "2073aee2-ad38-4602-a9bb-1517ee9bbc28", "status_msg": "This is a test, it should be successful", "status_msg_code": "ze0001", "status_type": "INFO", "status_zed_code": "200", "subject": "test",
# "timestamp": "2020-01-08T21:37:17.424278Z", "type": "Test"}
# # nest_prefix(event_dict,prefixes=["status"])
