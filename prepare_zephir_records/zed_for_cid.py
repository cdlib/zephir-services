import os
import sys

import datetime
import json
from csv import DictReader
import uuid

class ZedEvent(object):
    """A class for ZED event with following properties:
    Attributes:
    """
    def __init__():
        pass

def test_json():

    dictionary = {
      "name": "sathiyajith",
      "rollno": 56,
      "cgpa": 8.6,
      "phonenumber": "9976770500"
    }
    
    with open("sample.json", "w") as outfile:
        json.dump(dictionary, outfile)

"""ZED msg table: https://github.com/cdlib/htmm-env/blob/master/zed/msg_tables/columns_prep.csv
"""
def get_zed_msg_table(input_filename):
    with open(input_filename, 'r', newline='', encoding="utf-8-sig") as csvfile:
        reader = DictReader(csvfile)
        result = {}
        for row in reader:
            key = row.get('status_msg_code')
            if key in result:
                print(f"Duplicate key error: Zed msg table contains duplicated status_msg_code {key}")
            else:
                result[key] = row

        return result
 
def get_zed_event_default_values(zed_msg_table, status_msg_code):
    event_default_values = {}
    data_fields = ["status_zed_code", "status_msg", "status_type", "topic", "type", "action"]
    try:
        for key in data_fields:
            event_default_values[key] = zed_msg_table.get(status_msg_code).get(key)
        return event_default_values
    except Exception as ex:
        print(f"Zed msg table lookup error: {ex}")
        return None

"""ZED event specification https://docs.google.com/document/d/1vhMV3JGeMhrkNK1n0j05OFYsXgdQD0N_/edit
Data model (6/28/2018)
## [R]-Required, [O]-Optional
{
   "time":"<iso 8601 (2017-02-28T19:30:27+00:00)>[R]",
   "status":{
      "type":"<INFO|WARN|ERROR>[R]",
      "zed_code":"<code (123)>[R]",
      "msg_code":"<code (pk0001)>[R]",
      "msg":"<short description of event>"[O],
      "details":"<long explanation. 150 char limit>[O]",
      "stacktrace":"<stack backtrace. 150 char limit>"[O]
      },
   "report":"<json object of event-specific data ({"config_name":"ia-ucla","config":"list_of_the_config","config_date":"20160611"})>[O]"
   "action":"<process_event_action (SaveZephirRecord)>[R]",
   "type":<process_event_type (Gobblerun)>[R]",
   "subject":"<event from/old processing thing (file.xml.1)>[O]",
   "object":"<event to/new item processing thing (htid.1234567)>[O]",
   "topic":"Raw",
   "event":"<GUID-per-event (cbe8a9f5-4b1a-4da3-82f1-4df390b2fc43)[R]",
   "process":"<GUID-per-process. Shared with event when start or single event.(25476fc6-3047-4cc2-9a5e-49a649c14850)>[R]"
}

sample zed events
{
"timestamp":"2022-11-29T22:54:51.571362723Z",
"status":{
  "type":"INFO",
  "zed_code":"200",
  "msg_code":"pr0214",
  "msg":"Zephir record created in Preprun step"
  },
"report":{
  "CID":"102919969",
  "config_name":"hvd-3",
  "Rd_seq_no":"1909",
  "RecordID":"990004416720203941",
  "dig_src":"google",
  "collection":"HVD",
  "record_type":"update"HTID":"hvd.32044143743045"
  },
"action":"CreateRecord",
"type":"Preprun",
"subject":"hvd-3_20221121",
"object":"hvd.32044143743045",
"topic":"Raw",
"event":"d53254a2-7038-11ed-8148-8e463a075b05",
"process":"92b1be8b-5fa0-44e5-9796-0a2ab3a4d5f0"
}

{
"timestamp":"2022-11-29T22:54:51.577478594Z",
"status":{
  "type":"INFO",
  "zed_code":"200",
  "msg_code":"pr0215",
  "msg":"Gobble file created",
  "details":"INFO: Completed writing output file 1: input 1909, output 1909, filename ./process/20221129145120.hvd-3.hvd-3-hvd-3_20221121.1.xml"},
"report":{"config_name":"hvd-3"},
"action":"CreateGobbleFile",
"type":"Preprun",
"subject":"hvd-3_20221121",
"object":"./process/20221129145120.hvd-3.hvd-3-hvd-3_20221121.1.xml",
"topic":"Raw",
"event":"d5333390-7038-11ed-8215-8e463a075b05",
"process":"92b1be8b-5fa0-44e5-9796-0a2ab3a4d5f0"}
"""
def create_zed_event(zed_msg_table, status_msg_code, event_data):
    """
    """
    process_key = event_data.get("process_key")
    zed_msg_detail = event_data.get("msg_detail")
    zed_report = event_data.get("report")
    zed_subject = event_data.get("subject")
    zed_object = event_data.get("object");

    event_default_values = get_zed_event_default_values(zed_msg_table, status_msg_code)
    if not event_default_values:
        return "ERROR: Undefined message code: status_msg_code"

    event_key = str(uuid.uuid4())
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3]

    status = {
        "type": event_default_values.get("status_type"),
        "zed_code": event_default_values.get("status_zed_code"),
        "msg_code": status_msg_code,
        "msg": event_default_values.get("status_msg"),
        "details":"INFO: "
        }
    if event_data.get("msg_detail"):
        status["details"] = event_data.get("msg_detail") 

    zed_event = {
        "timestamp": timestamp,
        "status": status,
        "report":{"config_name":"hvd-3"},
        "action": event_default_values.get("action"),
        "type": event_default_values.get("type"),
        "subject": zed_subject,
        "object": zed_object,
        "topic": event_default_values.get("topic"),
        "event": event_key,
        "process": process_key
    }
    return zed_event

def main():
    #test_json()
    zed_msg_table_file= "/apps/htmm/cdl-env/zed/msg_tables/columns_prep.csv"
    zed_msg_table = get_zed_msg_table(zed_msg_table_file)
    print(type(zed_msg_table))
    print(zed_msg_table.keys())
    #print(zed_msg_table)
    msg_code = "pr0212" # assign new CID
    print(zed_msg_table.get(msg_code))
    print(get_zed_event_default_values(zed_msg_table, msg_code))
    print(get_zed_event_default_values(zed_msg_table, 'pr0001000'))

    event_data = {
        "process_key": "92b1be8b-5fa0-44e5-9796-0a2ab3a4d5f0",
        "msg_detail": "msg details",
        "report": {"CID": "123456789", "RecordID": "100001", "Rd_seq_no": "1", "config_name": "test_config"},
        "subject": "test subject",
        "object": "test object",
    }
    print( create_zed_event(zed_msg_table, msg_code, event_data))


if __name__ == '__main__':
    main()
