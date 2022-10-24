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
from typing import List, Optional, Dict, Any

import kubernetes.config.kube_config
from datadog_api_client import Configuration, ApiClient
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.http_log import HTTPLog
from datadog_api_client.v2.model.http_log_item import HTTPLogItem

from kubernetes import config
from kubernetes.config import ConfigException

from proteus.logs.models import ProteusLogMetadata
from proteus.utils import convert_datadog_tags


class DataDogApiHandler(Handler):
    """
      Logging handler for DataDog.
    """

    def __init__(self, *, buffer_size=10, async_handler=False, debug=False,
                 fixed_tags: Optional[Dict[str, str]] = None):
        """
          Creates a handler than can upload log records to DataDog index.

          Additional docs: https://docs.datadoghq.com/logs/log_collection/?tab=host#attributes-and-tags

        :param buffer_size: Optional number of records to buffer up in memory before sending to DataDog.
        :param async_handler: Whether to send requests in an async manner. Only use this for production.
        :param debug: Whether to print messages from this handler to the console. Use this to debug handler behaviour.
        :param fixed_tags: Static key-value pairs to be applied as tags for each log message.
          Some keys will be added if not present in this dictionary:
            - environment: Environment sending logs. If not provided, will be inferred depending on the actual runtime.
        """
        super().__init__()
        assert os.getenv('PROTEUS__DD_API_KEY'), 'PROTEUS__DD_API_KEY environment variable must be set in order to use DataDogApiHandler'
        assert os.getenv('PROTEUS__DD_APP_KEY'), 'PROTEUS__DD_APP_KEY environment variable must be set in order to use DataDogApiHandler'
        assert os.getenv('PROTEUS__DD_SITE'), 'PROTEUS__DD_SITE environment variable must be set in order to use DataDogApiHandler'

        configuration = Configuration()
        configuration.server_variables["site"] = os.getenv('PROTEUS__DD_SITE')
        configuration.api_key['apiKeyAuth'] = os.getenv('PROTEUS__DD_API_KEY')
        configuration.api_key['appKeyAuth'] = os.getenv('PROTEUS__DD_APP_KEY')

        if debug:
            configuration.debug = True

        self._logs_api = LogsApi(api_client=ApiClient(configuration))
        self._buffer: List[HTTPLogItem] = []
        self._buffer_size = buffer_size
        self._async_handler = async_handler
        self._debug = debug
        self._configuration = configuration

        # send records even if an application is interrupted
        if platform.system() != "Windows":
            signal.signal(signal.SIGINT, self._flush)
            signal.signal(signal.SIGTERM, self._flush)

        # environment tag is inferred from kubernetes context name, if one exists
        self._fixed_tags = fixed_tags or {}
        if 'environment' not in self._fixed_tags:
            self._fixed_tags.setdefault('environment', 'local')
            try:
                config.load_incluster_config()
                _, current_context = config.list_kube_config_contexts()
                assert isinstance(current_context, kubernetes.config.kube_config.ConfigNode)
                self._fixed_tags['environment'] = current_context.name
            except ConfigException:
                pass

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

            metadata: Optional[ProteusLogMetadata] = rec.__dict__.get(ProteusLogMetadata.__name__)
            tags = {}
            formatted_message: Dict[str, Any] = {
                "text": rec.getMessage()
            }
            if metadata:
                if metadata.tags:
                    tags.update(metadata.tags)
                for key, value in metadata.fields.items():
                    formatted_message[key] = value
                if metadata.template:
                    formatted_message["template"] = metadata.template
                if metadata.diagnostics:
                    formatted_message["diagnostics"] = metadata.diagnostics
                if metadata.exc_info:
                    formatted_message.setdefault('error', {
                        'stack': "".join(traceback.format_exception(*metadata.exc_info, chain=True)).strip("\n"),
                        'message': str(metadata.exception),
                        'kind': type(metadata.exception).__name__
                    })
            tags.update(self._fixed_tags)

            return HTTPLogItem(
                ddsource=rec.name,
                ddtags=','.join(convert_datadog_tags(tags)),
                hostname=socket.gethostname(),
                message=json.dumps(formatted_message),
                status=rec.levelname
            )

        if len(self._buffer) < self._buffer_size:
            self._buffer.append(convert_record(record))
        else:
            self._buffer.append(convert_record(record))
            self._flush()

    def flush(self) -> None:
        self._flush()
