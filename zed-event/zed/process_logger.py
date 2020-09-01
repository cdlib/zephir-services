from zed.event import ZedEvent

class ZedProcessLogger:
    """For creating Zed Process log JSON strings for logs and other services.

        Usage examples:

        zed_log = ZedProcessLogger(zed_table_path, options)

        zed_log.log_event("EventID", data={})
    """
    # INSTANCE-LEVEL BLOCK
    def __init__(self, table_path=None, delimiter=","):
        """ZedProcessLogger instance for creating ZED process JSON log lines.

        Args:
            table_path (str): The path to the csv table of ZED event default data
            delimiter (str): Delimiter for CSV file
        """
        self._table = ZedTable(table_path, delimiter)
        self._process = ZedEvent.generate_key()


    @property
    def process(self):
        return self._process

    def log(self, code, override={}):
        overide["process"] = self._process
        return ZedEvent(self._table.get(code), override)
