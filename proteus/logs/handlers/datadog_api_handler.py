import os
from logging import LogRecord, Handler
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.http_log import HTTPLog


class DataDogApiHandler(Handler):
    def __init__(self):
        super().__init__()
        assert os.getenv(
            'DATADOG_API_KEY'), 'DATADOG_API_KEY environment variable must be set in order to use DataDogApiHandler'
        self._logs_api = LogsApi()

    def emit(self, record: LogRecord) -> None:
        def convert_record(rec: LogRecord) -> HTTPLog:
            pass

        self._logs_api.submit_log(body=convert_record(record))  # TODO add retries

    def flush(self) -> None:
        pass
