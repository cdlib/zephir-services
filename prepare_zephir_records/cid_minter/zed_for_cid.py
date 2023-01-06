import os
import sys

from datetime import datetime
import json
from csv import DictReader
import uuid

class CidZedEvent(object):
    """A class for ZED event with following properties:
    Attributes:
    """
    def __init__(self, zed_msg_table_file, zed_event_log, event_data=None):
        self.zed_msg_table = self._get_zed_msg_table(zed_msg_table_file)
        self.zed_log_fp = open(zed_event_log, "w")
        self.zed_event_data = event_data
        self.zed_event = None

    """ZED msg table: https://github.com/cdlib/htmm-env/blob/master/zed/msg_tables/columns_prep.csv
    """
    def _get_zed_msg_table(self, input_filename):
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
 
    def _get_zed_event_default_values(self, status_msg_code):
        event_default_values = {}
        data_fields = ["status_zed_code", "status_msg", "status_type", "topic", "type", "action"]
        try:
            for key in data_fields:
                event_default_values[key] = self.zed_msg_table.get(status_msg_code).get(key)
            return event_default_values
        except Exception as ex:
            print(f"Zed msg table lookup error: {ex}")
            return None

    def setup_zed_event_data(self, event_data):
        self.zed_event_data = event_data

    def merge_zed_event_data(self, event_data):
        self.zed_event_data = self._merge_dict(self.zed_event_data, event_data)

    def _merge_dict(self, dict_1, dict_2):
        if dict_1 and dict_2:
            return {**dict_1, **dict_2}
        else:
            return dict_1 or dict_2

    def create_zed_event(self, status_msg_code):
        """Create ZED event using event data. Save event as self.zed_event and save it to zed log.

        ZED event specification https://docs.google.com/document/d/1vhMV3JGeMhrkNK1n0j05OFYsXgdQD0N_/edit
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
        """
        event_default_values = self._get_zed_event_default_values(status_msg_code)

        event_key = str(uuid.uuid4())
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3]

        status = {
            "type": event_default_values.get("status_type"),
            "zed_code": event_default_values.get("status_zed_code"),
            "msg_code": status_msg_code,
            "msg": event_default_values.get("status_msg"),
            }
        if self.zed_event_data.get("msg_detail"):
            status["details"] = self.zed_event_data.get("msg_detail") 

        zed_event = {
            "timestamp": timestamp,
            "status": status,
            "action": event_default_values.get("action"),
            "type": event_default_values.get("type"),
            "subject": self.zed_event_data.get("subject"),
            "object": self.zed_event_data.get("object"),
            "topic": event_default_values.get("topic"),
            "event": event_key,
            "process": self.zed_event_data.get("process_key"),
            "report": self._merge_dict(self.zed_event_data.get("ids"), self.zed_event_data.get("report"))
        }

        self.zed_event = zed_event
        json.dump(zed_event, self.zed_log_fp)
        self.zed_log_fp.write('\n')

    def close(self):
        self.zed_log_fp.close()

def main():
    zed_msg_table_file= "/apps/htmm/cdl-env/zed/msg_tables/columns_prep.csv"
    zed_log = "test_cid_zed_log.json"
    zed_event = CidZedEvent(zed_msg_table_file, zed_log)
   
    msg_code = "pr0212" # assign new CID
    event_data = {
        "process_key": "92b1be8b-5fa0-44e5-9796-0a2ab3a4d5f0",
        "msg_detail": "msg details",
        "report": {"CID": "123456789", "RecordID": "100001", "Rd_seq_no": "1", "config_name": "test_config"},
        "subject": "test subject",
        "object": "test object",
    }
    zed_event.merge_zed_event_data(event_data)
    zed_event.create_zed_event(msg_code)

    msg_code = "pr0213" # Assigned existing CID
    zed_event.create_zed_event(msg_code)

    zed_event.close()

if __name__ == '__main__':
    main()
