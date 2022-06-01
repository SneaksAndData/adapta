"""
  Logging handler for DataDog.
"""
import json
import os
import signal
import socket
import platform
import traceback
from logging import LogRecord, Handler
from typing import List

from datadog_api_client import Configuration, ApiClient
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.http_log import HTTPLog
from datadog_api_client.v2.model.http_log_item import HTTPLogItem


class DataDogApiHandler(Handler):
    """
      Logging handler for DataDog.
    """
    def __init__(self, *, buffer_size=10, async_handler=False, debug=False):
        """
          Creates a handler than can upload log records to DataDog index.

          Additional docs: https://docs.datadoghq.com/logs/log_collection/?tab=host#attributes-and-tags

        :param buffer_size: Optional number of records to buffer up in memory before sending to DataDog.
        :param async_handler: Whether to send requests in an async manner. Only use this for production.
        :param debug: Whether to print messages from this handler to the console. Use this to debug handler behaviour.
        """
        super().__init__()
        assert os.getenv(
            'DD_API_KEY'), 'DD_API_KEY environment variable must be set in order to use DataDogApiHandler'
        assert os.getenv('DD_APP_KEY', 'DD_APP_KEY environment variable must be set in order to use DataDogApiHandler')
        assert os.getenv('DD_SITE', 'DD_SITE environment variable must be set in order to use DataDogApiHandler')

        configuration = Configuration()
        configuration.server_variables["site"] = os.getenv('DD_SITE')

        if debug:
            configuration.debug = True

        self._logs_api = LogsApi(api_client=ApiClient(configuration))
        self._buffer: List[HTTPLogItem] = []
        self._buffer_size = buffer_size
        self._async_handler = async_handler
        self._debug = debug

        # send records even if an application is interrupted
        if platform.system() != "Windows":
            signal.signal(signal.SIGINT, self._flush)
            signal.signal(signal.SIGTERM, self._flush)

    def _flush(self) -> None:
        """
         Flushes a log buffer to the consumer

        :return:
        """
        result = self._logs_api.submit_log(
            body=HTTPLog(value=self._buffer),
            content_encoding='gzip',
            async_req=self._async_handler
        )
        if self._async_handler:
            result = result.get()

        if self._debug:
            print(f"DataDog response: {result}")

        self._buffer = []

    def emit(self, record: LogRecord) -> None:
        def convert_record(rec: LogRecord) -> HTTPLogItem:

            record_json = json.loads(self.format(rec))
            record_message = json.loads(record_json['message'])

            tags = record_json.get('tags', None)
            if tags:
                record_json.pop('tags')

            if rec.exc_info:
                ex_type, _, ex_tb = rec.exc_info
                record_message.setdefault('error', {
                    'stack': '\n'.join(traceback.format_tb(ex_tb)),
                    'message': rec.exc_text,
                    'kind': ex_type.__name__
                })

            return HTTPLogItem(
                ddsource=rec.name,
                ddtags=tags,
                hostname=socket.gethostname(),
                message=json.dumps(record_message),
                status=rec.levelname
            )

        if len(self._buffer) < self._buffer_size:
            self._buffer.append(convert_record(record))
        else:
            self._buffer.append(convert_record(record))
            self._flush()

    def flush(self) -> None:
        self._flush()
