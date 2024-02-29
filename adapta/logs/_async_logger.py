"""
 Asyncio-safe implementation of a Semantic Logger.
"""
import asyncio

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

import logging
import sys
from contextlib import asynccontextmanager
from logging.handlers import QueueHandler, QueueListener
from multiprocessing import Queue
from typing import final, TypeVar, Generic, Type, List, Optional, Dict

from adapta.logs._internal import MetadataLogger
from adapta.logs._internal_logger import _InternalLogger
from adapta.logs.models import LogLevel

TLogger = TypeVar("TLogger")  # pylint: disable=C0103


@final
class _AsyncLogger(Generic[TLogger], _InternalLogger):
    """
    Asyncio-safe wrapper for MetadataLogger
    """

    def redirect(self, tags: Optional[Dict[str, str]] = None, **kwargs):
        return self._redirect(logger=self._logger, tags=tags)

    @asynccontextmanager
    async def redirect_async(self, tags: Optional[Dict[str, str]] = None, **kwargs):  # pylint: disable=R0801
        is_active = False
        tmp_symlink = b""

        async def log_redirected() -> int:
            def flush_and_log(pos: int) -> int:
                sys.stdout.flush()
                with open(tmp_symlink, encoding="utf-8") as output:
                    output.seek(pos)
                    for line in output.readlines():
                        self._log_redirect_message(
                            self._logger, base_template="Redirected output: {message}", message=line, tags=tags
                        )
                    return output.tell()

            start_position = 0
            # externally control flush activation
            while tmp_symlink == b"":
                await asyncio.sleep(0.1)

            # externally control the flushing process
            while is_active:
                start_position = flush_and_log(start_position)
                await asyncio.sleep(0.1)

            return flush_and_log(start_position)

        self._handle_unsupported_redirect(tags)
        libc, saved_stdout, tmp_file = self._prepare_redirect()
        read_task = asyncio.create_task(log_redirected())
        try:
            tmp_symlink = self._activate_redirect(libc, tmp_file)
            is_active = True
            yield None
        except Exception as ex:
            raise ex
        finally:
            is_active = False
            _ = await read_task
            self._close_redirect(libc, saved_stdout)

    def __init__(
        self,
        name: str,
        min_log_level: LogLevel,
        log_handlers: List[logging.Handler],
        fixed_template: Optional[Dict[str, Dict[str, str]]] = None,
        fixed_template_delimiter=", ",
    ):
        super().__init__(fixed_template, fixed_template_delimiter)
        self._logger: MetadataLogger = logging.getLogger(name)
        self._logger.setLevel(min_log_level.value)
        self._logger_message_queue = Queue(-1)
        self._logger_queue_handler = QueueHandler(self._logger_message_queue)
        self._logger.addHandler(self._logger_queue_handler)
        self._log_handlers = log_handlers
        self._listener: Optional[QueueListener] = None

    def info(
        self,
        template: str,
        tags: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> None:
        """
          Sends an INFO level message to configured log sources.

        :param template: Message template.
        :param tags: Optional message tags.
        :param kwargs: Templated arguments (key=value).
        :return:
        """
        self._meta_info(template=template, logger=self._logger, tags=tags, **kwargs)

    def warning(
        self,
        template: str,
        exception: Optional[BaseException] = None,
        tags: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> None:
        """
          Sends a WARNING level message to configured log sources.

        :param template: Message template.
        :param exception: Exception associated with this warning.
        :param tags: Optional message tags.
        :param kwargs: Templated arguments (key=value).
        :return:
        """
        self._meta_warning(template=template, logger=self._logger, tags=tags, exception=exception, **kwargs)

    def error(
        self,
        template: str,
        exception: Optional[BaseException] = None,
        tags: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> None:
        """
          Sends an ERROR level message to configured log sources.

        :param template: Message template.
        :param exception: Exception associated with this error.
        :param tags: Optional message tags.
        :param kwargs: Templated arguments (key=value).
        :return:
        """
        self._meta_error(template=template, logger=self._logger, tags=tags, exception=exception, **kwargs)

    def debug(
        self,
        template: str,
        exception: Optional[BaseException] = None,
        diagnostics: Optional[str] = None,  # pylint: disable=R0913
        tags: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> None:
        """
          Sends a DEBUG level message to configured log sources.

        :param template: Message template.
        :param exception: Exception associated with this error.
        :param diagnostics: Optional additional diagnostics info.
        :param tags: Optional message tags.
        :param kwargs: Templated arguments (key=value).
        :return:
        """
        self._meta_debug(
            template=template, logger=self._logger, tags=tags, exception=exception, diagnostics=diagnostics, **kwargs
        )

    def start(self):
        """
        Starts the async listener.
        """
        self._listener = QueueListener(self._logger_message_queue, *self._log_handlers, respect_handler_level=True)
        self._listener.start()

    def stop(self):
        """
        Stops the async listener and flushes the buffer out to all handlers.
        """
        self._listener.stop()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


def create_async_logger(
    logger_type: Type[TLogger],
    log_handlers: List[logging.Handler],
    min_log_level: LogLevel = LogLevel.INFO,
    fixed_template: Optional[Dict[str, Dict[str, str]]] = None,
    fixed_template_delimiter=", ",
) -> _AsyncLogger[TLogger]:
    """
    Factory method to create an async-io safe logger.
    """
    logger_name = f"{logger_type.__module__}.{logger_type.__qualname__}"
    logging.setLoggerClass(MetadataLogger)
    return _AsyncLogger[TLogger](
        name=logger_name,
        min_log_level=min_log_level,
        log_handlers=log_handlers,
        fixed_template=fixed_template,
        fixed_template_delimiter=fixed_template_delimiter,
    )
