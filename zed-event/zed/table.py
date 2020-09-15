import json
import re

import numpy as np
import pandas as pd

from zed.event import ZedEvent
from zed.error import *

class ZedTable:
    """
        Create ZedTable to lookup ZedEvent data by status_msg_code or status_shorthand.

        Usage examples:

        table = ZedTable(file_path)

        zed_event = table.get("ZEDEVENT_CODE")
    """
    def __init__(self, file_path=None, delimiter=","):
        """
            New Zed Table for lookup of Zed Event values
            Args:
                file_path (str): Path to CSV data
                delimiter (str): Delimitr for CSV data
        """
        if file_path:
            self._table_df = pd.read_csv(file_path, delimiter=delimiter, dtype=str)
        else:
            self._table_df = pd.DataFrame()
        self.table = self._table_df
        self.headers = list(self._table_df.columns)

    def get(self, key):
        """
            Get Zed Event data given key
            Args:
                key (str): Zed status msg_code or shorthand of Zed Event
            Returns: Zed Data (dict)
        """
        events = pd.DataFrame()
        if "status_msg_code" in self.headers:
            events = self._table_df.loc[self._table_df["status_msg_code"] == key]
        if events.empty and "status_shorthand" in self.headers:
            # check shortand if not found by status_msg_code
            events = self._table_df.loc[self._table_df["status_shorthand"] == key]
        if not events.empty:
            return self._unflatten(events.iloc[0].to_dict())
        else:
            raise ZedEventNotFoundError(key, "Event '{}' not found in Zed Table".format(key))

    def _unflatten(self, flat_event):
        """
            Convert flat Zed Event data to multidemensional Zed Event data
            Args:
                flat_event (dict): Flat data representing ZedEvent
            Returns: Multidemensional Zed Data (dict)
        """
        event_dict = {}
        if flat_event.get("action"):
            event_dict["action"] = flat_event.get("action")
        if flat_event.get("object"):
            event_dict["object"] = flat_event.get("object")
        event_dict["status"] = {}
        if flat_event.get("status_msg"):
            event_dict["status"]["msg"] = flat_event.get("status_msg")
        if flat_event.get("status_msg_code"):
            event_dict["status"]["msg_code"] = flat_event.get("status_msg_code")
        if flat_event.get("status_type"):
            event_dict["status"]["type"] = flat_event.get("status_type")
        if flat_event.get("status_msg"):
            event_dict["status"]["zed_code"] = flat_event.get("status_zed_code")
        if flat_event.get("subject"):
            event_dict["subject"] = flat_event.get("subject")
        if flat_event.get("topic"):
            event_dict["topic"] = flat_event.get("topic")
        if flat_event.get("type"):
            event_dict["type"] = flat_event.get("type")
        return event_dict
