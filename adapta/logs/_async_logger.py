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
import threading
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
    async def redirect_async(self, tags: Optional[Dict[str, str]] = None, **kwargs):
        is_active = False
        tmp_symlink_out = b""
        tmp_symlink_err = b""

        async def log_redirected() -> tuple[int, int]:
            start_position_out = 0
            start_position_err = 0
            # externally control flush activation
            while tmp_symlink_out == b"" and tmp_symlink_err == b"":
                await asyncio.sleep(0.1)

            # externally control the flushing process
            while is_active:
                start_position_out = self._flush_and_log(
                    pos=start_position_out, tmp_symlink=tmp_symlink_out, logger=self._logger, tags=tags
                )
                start_position_err = self._flush_and_log(
                    pos=start_position_err, tmp_symlink=tmp_symlink_err, logger=self._logger, tags=tags
                )
                await asyncio.sleep(0.1)

            return self._flush_and_log(
                pos=start_position_out, tmp_symlink=tmp_symlink_out, logger=self._logger, tags=tags
            ), self._flush_and_log(pos=start_position_err, tmp_symlink=tmp_symlink_err, logger=self._logger, tags=tags)

        self._handle_unsupported_redirect(tags)
        libc, saved_stdout, saved_stderr, tmp_file_out, tmp_file_err = self._prepare_redirect()
        read_task = asyncio.create_task(log_redirected())
        try:
            tmp_symlink_out, tmp_symlink_err = self._activate_redirect(libc, tmp_file_out, tmp_file_err)
            is_active = True
            yield None
        except Exception as ex:
            raise ex
        finally:
            is_active = False
            _ = await read_task
            self._close_redirect(libc, saved_stdout, saved_stderr)

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
        self._is_active: bool = False
        self._lock = threading.RLock()

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
        with self._lock:
            if not self._is_active:
                self._listener = QueueListener(
                    self._logger_message_queue, *self._log_handlers, respect_handler_level=True
                )
                self._listener.start()
                self._is_active = True

    def stop(self):
        """
        Stops the async listener and flushes the buffer out to all handlers.
        """
        with self._lock:
            if self._is_active:
                self._listener.stop()
                self._is_active = False

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
