import json
import re

import numpy as np
import pandas as pd

from zed_event import ZedEvent

class ZedTable:
    def __init__(self, dataframe):
        self.table = dataframe.values
        self.header = list(dataframe.columns)
        #self.header = "status_msg_code","status_msg_name","status_zed_code","status_msg","topic","type","action","subject","object"


    def create_event(self, code, data={}, init=["event", "process", "timestamp"], validate=None):
        coded_list = self.table[self.table[:,1]==code][0]
        coded_defaults = self.flat_to_dict(dict(zip(self.header, coded_list))) #dict(zip(self.header, coded_list))
        coded_event = ZedEvent({**coded_defaults, **data}, init, validate)
        print(dict(zip(self.header, coded_list)))
        print(coded_event)
        return coded_event

    def flat_to_dict(self, event_csv):
        event_dict = {
            "event": event_csv.get("event"),
            "object": event_csv.get("object"),
            "process": event_csv.get("process"),
            "report": event_csv.get("report"),
            "status": {
                "msg": event_csv.get("status_msg"),
                "msg_code": event_csv.get("status_msg_code"),
                "type": event_csv.get("status_type"),
                "zed_code": event_csv.get("status_zed_code")
                },
            "subject": event_csv.get("subject"),
            "timestamp": event_csv.get("timestamp"),
            "topic": event_csv.get("topics"),
            "type": event_csv.get("topic"),
            }
        return event_dict

import re


def nest_prefix(data, prefixes=[]):
    new_dict = {}
    for k, v in data.items():
        for prefix in prefixes:
            if re.match("^"+prefix+"_", k):
                result = nest_prefix({k.split(prefix+"_")[1]:v},prefixes)
                if prefix in new_dict:
                    for k, v in result.items():
                        new_dict[prefix][k] = v
                else:
                    new_dict[prefix] = result
            else:
                new_dict[k]=v
    return new_dict

def nest_dict(data, prefixes=[]):
    new_dict = {}
    for key in data:
        for prefix in prefixes:
            if re.match("^"+prefix+"_", key):
                result = nest_dict({key.split(prefix+"_")[1]:data[key]},prefixes)
                if prefix in new_dict:
                    for key in result:
                        new_dict[prefix][key] = result[key]
                else:
                    new_dict[prefix] = result
            else:
                new_dict[key]=data[key]
    return new_dict

    def nest_dict_compact(data, prefixes=[]):
    new_dict = {}
    for key in data:
        match = re.match("^(\w+?)_", key)
        if match:
            prefix = match.group(1)
            if prefix in prefixes:
                result = nest_dict_compact({key.split(prefix+"_")[1]:data[key]},prefixes)
                if prefix in new_dict:
                    for key in result:
                        new_dict[prefix][key] = result[key]
                else:
                    new_dict[prefix] = result
            else:
                new_dict[key]=data[key]
    return new_dict

{"event": "30be6cd1-02ba-47e9-be0d-988561aeafea", "object": "test",
"process": "2073aee2-ad38-4602-a9bb-1517ee9bbc28", "status_msg": "This is a test, it should be successful", "status_msg_code": "ze0001", "status_type": "INFO", "status_zed_code": "200", "subject": "test",
"timestamp": "2020-01-08T21:37:17.424278Z", "type": "Test"}
# nest_prefix(event_dict,prefixes=["status"])
