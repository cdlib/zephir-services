from datetime import datetime
import logging

logger = logging.getLogger("TACT Logger")

class RunReport:
    def __init__(self, publisher='', filename=''):
        self.publisher = publisher
        self.filename = filename
        self.run_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        self.input_records = 0
        self.total_processed_records = 0
        self.rejected_records = 0
        self.new_records_added = 0
        self.existing_records_updated = 0
        self.status = 'S'
        self.error_msg = ''

    def display(self):
        status_map = {
            'S': "Success",
            'F': "Failed"
        }

        logger.info("Status: {}".format(status_map.get(self.status, '')))
        if self.error_msg:
            logger.info("Error message: {}".format(self.error_msg))
        logger.info("Publisher: {}".format(self.publisher))
        logger.info("Filename: {}".format(self.filename))
        logger.info("Run datatime: {}".format(self.run_datetime))
        logger.info("Input Records: {}".format(self.input_records))
        logger.info("Total Processed Records: {}".format(self.total_processed_records))
        logger.info("Rejected Records: {}".format(self.rejected_records))
        logger.info("New Records Added: {}".format(self.new_records_added))
        logger.info("Existing Records Updated: {}".format(self.existing_records_updated))
