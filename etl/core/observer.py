from typing import Dict
from datetime import datetime
from etl.utils.utils import setup_logger


class ETLObserver:
    """
    Observer for ETL processes that handles monitoring, logging, and metrics collection.
    """

    def __init__(self):
        self.logger = setup_logger("etl_observer.log")
        self.metrics = {
            'start_time': None,
            'end_time': None,
            'records_processed': 0,
            'errors': [],
            'steps_completed': []
        }

    def update(self, message: str, step_type: str = 'info'):
        """
        Receives updates from the ETL pipeline and processes them accordingly.

        Args:
            message (str): The message to process
            step_type (str): Type of step ('extract', 'transform', 'load', 'info', 'error')
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if step_type == 'info' and 'starting' in message.lower():
            self.metrics['start_time'] = datetime.now()

        if step_type == 'info' and 'completed' in message.lower():
            self.metrics['end_time'] = datetime.now()

        if step_type == 'extract':
            count = self._extract_count(message)
            self.metrics['records_processed'] = count
            self.metrics['steps_completed'].append('extraction')

        if step_type == 'error':
            self.metrics['errors'].append(message)
            self.logger.error(f"{timestamp} - {message}")
        else:
            self.logger.info(f"{timestamp} - {message}")
    def get_metrics(self) -> Dict:
        """
        Returns the collected metrics for the ETL process.
        """
        if self.metrics['start_time'] and self.metrics['end_time']:
            duration = self.metrics['end_time'] - self.metrics['start_time']
            self.metrics['duration'] = str(duration)

        return self.metrics

    @staticmethod
    def _extract_count(message: str) -> int:
        """
        Extracts the number of records from a message.
        """
        try:
            return int(''.join(filter(str.isdigit, message)))
        except ValueError:
            return 0
