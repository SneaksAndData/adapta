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

    def add_log_source(self, *, log_source: str, lowest_log_level: LogLevel,
                       log_handlers: Optional[List[Handler]] = None) -> 'ProteusLogger':
        """
          Adds a new log source.

        :param log_source: Name of a source.
        :param lowest_log_level: Minimal log level for this source.
        :param log_handlers: Attached log handlers. StreamHandler is used if not provided
        :return:
        """
        new_logger = logging.getLogger(log_source)
        new_logger.setLevel(lowest_log_level.value)

        if not log_handlers or len(log_handlers) == 0:
            log_handlers = [StreamHandler(sys.stdout)]

        for log_handler in log_handlers:
            log_handler.setFormatter(json_log_formatter.JSONFormatter())
            new_logger.addHandler(log_handler)

        self._loggers.setdefault(log_source, new_logger)

        return self

    def __getattr__(self, log_source) -> Optional[logging.Logger]:
        if log_source in self._loggers:
            return self._loggers[log_source]

        return None

    def log(self, log_source: str, data: BaseLog) -> None:
        """
          Send a log message.

        :param log_source: Source of a log message.
        :param data: Log data to send.
        :return:
        """

        assert log_source in self._loggers, f"{log_source} does not have an associated logger. Use add_log_source() to associate a logger with this log source."

        logger: logging.Logger = self._loggers[log_source]

        if isinstance(data, InfoLog):
            logger.info(msg=data.get_message())
        elif isinstance(data, WarnLog):
            logger.warning(msg=data.get_message(), exc_info=data.exception, stack_info=True, stacklevel=3)
        elif isinstance(data, ErrorLog):
            logger.error(msg=data.get_message(), exc_info=data.exception, stack_info=True, stacklevel=3)
        elif isinstance(data, DebugLog):
            logger.debug(msg=data.get_message(), exc_info=data.exception, stack_info=True, stacklevel=3,
                         extra=data.diagnostics)