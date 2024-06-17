"""
  Logging handler for DataDog.
"""
#  Copyright (c) 2023-2024. ECCO Sneaks & Data
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import base64
import json
import logging
import os
import signal
import socket
import platform
import traceback
from json import JSONDecodeError
from logging import LogRecord, Handler
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse

import backoff
from datadog_api_client import Configuration, ApiClient
from datadog_api_client.exceptions import ServiceException, ApiException
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.content_encoding import ContentEncoding
from datadog_api_client.v2.model.http_log import HTTPLog
from datadog_api_client.v2.model.http_log_item import HTTPLogItem

from urllib3.exceptions import HTTPError

from adapta.logs.models import CompositeLogMetadata
from adapta.utils import convert_datadog_tags


class DataDogApiHandler(Handler):
    """
    Logging handler for DataDog.
    """

    def __init__(
        self,
        *,
        buffer_size=0,
        debug=False,
        max_flush_retry_time=30,
        ignore_flush_failure=True,
        fixed_tags: Optional[Dict[str, str]] = None,
    ):
        """
          Creates a handler than can upload log records to DataDog index.

          Additional docs: https://docs.datadoghq.com/logs/log_collection/?tab=host#attributes-and-tags

        :param buffer_size: Optional number of records to buffer up in memory before sending to DataDog.
        :param debug: Whether to print messages from this handler to the console. Use this to debug handler behaviour.
        :param max_flush_retry_time: Maximum time to spend retrying message flushes in case of connection failures.
        :param ignore_flush_failure: Whether to ignore log flush failure or raise an exception.
        :param fixed_tags: Static key-value pairs to be applied as tags for each log message.
          Some keys will be added if not present in this dictionary:
            - environment: Environment sending logs. If not provided, will be inferred depending on the actual runtime.
        """
        super().__init__()
        assert os.getenv(
            "PROTEUS__DD_API_KEY"
        ), "PROTEUS__DD_API_KEY environment variable must be set in order to use DataDogApiHandler"
        assert os.getenv(
            "PROTEUS__DD_APP_KEY"
        ), "PROTEUS__DD_APP_KEY environment variable must be set in order to use DataDogApiHandler"
        assert os.getenv(
            "PROTEUS__DD_SITE"
        ), "PROTEUS__DD_SITE environment variable must be set in order to use DataDogApiHandler"

        configuration = Configuration()
        configuration.server_variables["site"] = os.getenv("PROTEUS__DD_SITE")
        configuration.api_key["apiKeyAuth"] = os.getenv("PROTEUS__DD_API_KEY")
        configuration.api_key["appKeyAuth"] = os.getenv("PROTEUS__DD_APP_KEY")

        if debug:
            configuration.debug = True

        self._logs_api = LogsApi(api_client=ApiClient(configuration))
        self._buffer: List[HTTPLogItem] = []
        self._buffer_size = buffer_size
        self._debug = debug
        self._configuration = configuration

        # send records even if an application is interrupted
        self._attach_interrupt_handlers()

        # environment tag is inferred from kubernetes context name, if one exists
        self._fixed_tags = fixed_tags or {}
        if "environment" not in self._fixed_tags:
            self._fixed_tags.setdefault("environment", "local")
            try:
                with open("/var/run/secrets/kubernetes.io/serviceaccount/token", "r", encoding="utf-8") as token_file:
                    issued_jwt = token_file.readline()
                    issuer_url = urlparse(
                        json.loads(base64.b64decode(issued_jwt.split(".")[1] + "==").decode("utf-8"))["iss"]
                    )
                    env = issuer_url.netloc
                    if issuer_url.path:
                        env = env + issuer_url.path
                self._fixed_tags["environment"] = env or self._fixed_tags["environment"]
            except (JSONDecodeError, FileNotFoundError):
                pass

        self._max_flush_retry_time = max_flush_retry_time
        self._ignore_flush_failure = ignore_flush_failure

    def _attach_interrupt_handlers(self) -> None:
        # Windows is normally used only on developer workstations
        # thus log flush is not important to do
        if platform.system() == "Windows":
            return

        # save existing handlers that might have been set by user code
        self._existing_sigterm_handler = signal.getsignal(signal.SIGTERM)
        self._existing_sigint_handler = signal.getsignal(signal.SIGINT)

        # attach custom handler to flush buffered log records before terminating the app
        signal.signal(signal.SIGTERM, self._handle_interrupt)
        signal.signal(signal.SIGINT, self._handle_interrupt)

    def _handle_interrupt(self, sig_num: int, stack_frame: Any) -> None:
        # flush remaining records in the buffer
        self.flush()

        # call saved handler
        if (
            sig_num == signal.SIGTERM
            and self._existing_sigterm_handler is not None
            and callable(self._existing_sigterm_handler)
        ):
            return self._existing_sigterm_handler(sig_num, stack_frame)
        if (
            sig_num == signal.SIGINT
            and self._existing_sigint_handler is not None
            and callable(self._existing_sigint_handler)
        ):
            return self._existing_sigint_handler(sig_num, stack_frame)

        return None

    def _flush(self) -> None:
        """
         Flushes a log buffer to the consumer

        :return:
        """

        @backoff.on_exception(
            wait_gen=backoff.expo,
            exception=(
                ConnectionResetError,
                ConnectionRefusedError,
                ConnectionAbortedError,
                ConnectionError,
                HTTPError,
                ServiceException,
                ApiException,
            ),
            max_time=self._max_flush_retry_time,
            raise_on_giveup=not self._ignore_flush_failure,
        )
        def _try_flush():
            result = self._logs_api.submit_log(
                body=HTTPLog(value=self._buffer),
                content_encoding=ContentEncoding.GZIP,
            )

            if self._debug:
                print(f"DataDog response: {result}")

        logger = logging.getLogger("urllib3")
        old_level = logger.getEffectiveLevel()
        logger.setLevel(logging.INFO)

        self.acquire()
        try:
            _try_flush()
        finally:
            self.release()
            logger.setLevel(old_level)

        self._buffer = []

    def emit(self, record: LogRecord) -> None:
        def convert_record(rec: LogRecord) -> HTTPLogItem:
            metadata: Optional[CompositeLogMetadata] = rec.__dict__.get(CompositeLogMetadata.__name__)
            tags = {}
            formatted_message: Dict[str, Any] = {"text": rec.getMessage()}
            if metadata:
                if metadata.tags:
                    tags.update(metadata.tags)
                for key, value in metadata.fields.items():
                    formatted_message[key] = value
                if metadata.template:
                    formatted_message["template"] = metadata.template
                if metadata.diagnostics:
                    formatted_message["diagnostics"] = metadata.diagnostics
            if rec.exc_info:
                ex_type, ex_value, _ = rec.exc_info
                formatted_message.setdefault(
                    "error",
                    {
                        "stack": "".join(traceback.format_exception(*rec.exc_info, chain=True)).strip("\n"),
                        "message": str(ex_value),
                        "kind": ex_type.__name__,
                    },
                )
            tags.update(self._fixed_tags)

            return HTTPLogItem(
                ddsource=rec.name,
                ddtags=",".join(convert_datadog_tags(tags)),
                hostname=socket.gethostname(),
                message=json.dumps(formatted_message),
                status=rec.levelname,
            )

        if len(self._buffer) < self._buffer_size:
            self._buffer.append(convert_record(record))
        else:
            self._buffer.append(convert_record(record))
            self._flush()

    def flush(self) -> None:
        self._flush()
