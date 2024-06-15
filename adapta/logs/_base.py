"""
 Adapta Logging Interface.
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

import logging
from logging import Handler, StreamHandler
from typing import List, Optional, Dict, final

from adapta.logs._internal_logger import _InternalLogger
from adapta.logs.models import LogLevel
from adapta.logs._internal import MetadataLogger


@final
class SemanticLogger(_InternalLogger):
    """
    Proxy for a collection of python loggers that use the same formatting interface.
    """

    async def redirect_async(self, tags: Optional[Dict[str, str]] = None, **kwargs):
        raise NotImplementedError("Async operations are not supported by this logger")

    def redirect(self, tags: Optional[Dict[str, str]] = None, log_source_name: Optional[str] = None, **kwargs):
        return self._redirect(logger=self._get_logger(log_source_name), tags=tags)

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def __init__(self, fixed_template: Optional[Dict[str, Dict[str, str]]] = None, fixed_template_delimiter=", "):
        """
          Creates a new instance of a SemanticLogger

        :param fixed_template: Additional template to append to message templates provided via logging methods.
        :param fixed_template_delimiter: Optional delimiter to use when appending fixed templates.
        """
        super().__init__(fixed_template, fixed_template_delimiter)
        self._loggers: Dict[str, logging.Logger] = {}
        self._default_log_source = None
        self._fixed_template = fixed_template
        self._fixed_template_delimiter = fixed_template_delimiter
        logging.setLoggerClass(MetadataLogger)

    def add_log_source(
        self,
        *,
        log_source_name: str,
        min_log_level: LogLevel,
        log_handlers: Optional[List[Handler]] = None,
        is_default=False,
    ) -> "SemanticLogger":
        """
          Adds a new log source.

        :param log_source_name: Name of a source.
        :param min_log_level: Minimal log level for this source.
        :param log_handlers: Attached log handlers. StreamHandler is used if not provided.
        :param is_default: Whether this log source should be used in a `log` method when no log source is explicitly provided.
        :return:
        """
        new_logger = logging.getLogger(log_source_name)
        new_logger.setLevel(min_log_level.value)

        if not log_handlers or len(log_handlers) == 0:
            log_handlers = [StreamHandler()]

        self._loggers.setdefault(log_source_name, new_logger)

        if is_default:
            self._default_log_source = log_source_name

        for log_handler in log_handlers:
            new_logger.addHandler(log_handler)

        return self

    def __getattr__(self, log_source) -> Optional[logging.Logger]:
        if log_source in self._loggers:
            return self._loggers[log_source]

        return None

    def _get_logger(self, log_source_name: Optional[str] = None) -> MetadataLogger:
        """
          Retrieves a logger by log source name, or a default logger is log source name is not provided.

        :param log_source_name: Optional name of a log source.
        :return:
        """
        assert (
            log_source_name or self._default_log_source
        ), "Argument `log_source` must be provided when no default log source is added. You can add a log source as default by calling `add_log_source(.., is_default=True)`"

        if log_source_name:
            assert (
                log_source_name in self._loggers
            ), f"{log_source_name} does not have an associated logger. Use add_log_source() to associate a logger with this log source."

        return self._loggers[log_source_name or self._default_log_source]

    def info(
        self,
        template: str,
        tags: Optional[Dict[str, str]] = None,
        log_source_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
          Sends an INFO level message to configured log sources.

        :param template: Message template.
        :param tags: Optional message tags.
        :param log_source_name: Optional name of a log source, if not using a default.
        :param kwargs: Templated arguments (key=value).
        :return:
        """
        logger = self._get_logger(log_source_name)
        self._meta_info(template=template, logger=logger, tags=tags, **kwargs)

    def warning(
        self,
        template: str,
        exception: Optional[BaseException] = None,
        tags: Optional[Dict[str, str]] = None,
        log_source_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
          Sends a WARNING level message to configured log sources.

        :param template: Message template.
        :param exception: Exception associated with this warning.
        :param tags: Optional message tags.
        :param log_source_name: Optional name of a log source, if not using a default.
        :param kwargs: Templated arguments (key=value).
        :return:
        """
        logger = self._get_logger(log_source_name)
        self._meta_warning(template=template, logger=logger, tags=tags, exception=exception, **kwargs)

    def error(
        self,
        template: str,
        exception: Optional[BaseException] = None,
        tags: Optional[Dict[str, str]] = None,
        log_source_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
          Sends an ERROR level message to configured log sources.

        :param template: Message template.
        :param exception: Exception associated with this error.
        :param tags: Optional message tags.
        :param log_source_name: Optional name of a log source, if not using a default.
        :param kwargs: Templated arguments (key=value).
        :return:
        """
        logger = self._get_logger(log_source_name)
        self._meta_error(template=template, logger=logger, tags=tags, exception=exception, **kwargs)

    def debug(
        self,
        template: str,
        exception: Optional[BaseException] = None,
        diagnostics: Optional[str] = None,  # pylint: disable=R0913
        tags: Optional[Dict[str, str]] = None,
        log_source_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
          Sends a DEBUG level message to configured log sources.

        :param template: Message template.
        :param exception: Exception associated with this error.
        :param diagnostics: Optional additional diagnostics info.
        :param tags: Optional message tags.
        :param log_source_name: Optional name of a log source, if not using a default.
        :param kwargs: Templated arguments (key=value).
        :return:
        """
        logger = self._get_logger(log_source_name)
        self._meta_debug(
            template=template, logger=logger, tags=tags, exception=exception, diagnostics=diagnostics, **kwargs
        )
