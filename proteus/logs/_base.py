"""
 Proteus Logging Interface.
"""
import logging
import sys

from logging import Handler, StreamHandler
from typing import List, Optional

import json_log_formatter

from proteus.logs.models import InfoLog, WarnLog, ErrorLog, DebugLog, BaseLog, LogLevel


class ProteusLogger:
    """
     Proteus Proxy for Python logging library.
    """

    def __init__(self):
        """
          Creates a new instance of a ProteusLogger
        """
        self._loggers = {}
        self._default_log_source = None

    def add_log_source(self, *, log_source_name: str, min_log_level: LogLevel,
                       log_handlers: Optional[List[Handler]] = None,
                       is_default=False) -> 'ProteusLogger':
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
            log_handlers = [StreamHandler(sys.stdout)]

        for log_handler in log_handlers:
            log_handler.setFormatter(json_log_formatter.JSONFormatter())
            new_logger.addHandler(log_handler)

        self._loggers.setdefault(log_source_name, new_logger)

        if is_default:
            self._default_log_source = log_source_name

        return self

    def __getattr__(self, log_source) -> Optional[logging.Logger]:
        if log_source in self._loggers:
            return self._loggers[log_source]

        return None

    def log(self, data: BaseLog, log_source_name: Optional[str] = None) -> None:
        """
          Send a log message from a provided or a default log source.

        :param log_source_name: Source of a log message.
        :param data: Log data to send.
        :return:
        """

        assert log_source_name or self._default_log_source, 'Argument `log_source` must be provided when no default log source is added. You can add a log source as default by calling `add_log_source(.., is_default=True)`'

        if log_source_name:
            assert log_source_name in self._loggers, f"{log_source_name} does not have an associated logger. Use add_log_source() to associate a logger with this log source."

        logger: logging.Logger = self._loggers[log_source_name or self._default_log_source]

        if isinstance(data, InfoLog):
            logger.info(msg=data.get_message())
        elif isinstance(data, WarnLog):
            logger.warning(msg=data.get_message(), exc_info=data.exception, stack_info=True, stacklevel=3)
        elif isinstance(data, ErrorLog):
            logger.error(msg=data.get_message(), exc_info=data.exception, stack_info=True, stacklevel=3)
        elif isinstance(data, DebugLog):
            logger.debug(msg=data.get_message(), exc_info=data.exception, stack_info=True, stacklevel=3)
