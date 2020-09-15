from zed.event import ZedEvent
from zed.table import ZedTable

class ZedProcess:
    """
        Create Zed Process with related Zed Event JSON strings for logs.

        Usage examples:

        process = ZedProcess(process_data)

        process.create_event(event_data={})
    """
    # INSTANCE-LEVEL BLOCK
    def __init__(self, process_data={}):
        """
            New zed process creating associated ZED events

            Args:
                process_data (dict): Process zed data
        """
        self.__process = ZedEvent.generate_id()
        self.__process_data =  {**process_data, **{"process":self.__process}}

    @property
    def id(self):
        """
            Returns: Process GUID (str)
        """
        return self.__process

    @property
    def data(self):
        """
            Returns: The Zed Event data (dict)
        """
        return self.__process_data

    def create_event(self, event_data={}):
        """
            Create a new ZED event

            Args:
                event_data (dict): Event specific data

            Returns: New Zed Event (ZedEvent)
        """
        return ZedEvent({**self.data, **event_data})
