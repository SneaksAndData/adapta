import logging
import sys

from logging import Handler, StreamHandler
from typing import List, Optional

import json_log_formatter

from proteus.logs.models.log_level import LogLevel
from proteus.logs.models.log_info import LogEntry


class ProteusLogger:
    def __init__(self, *, log_source: str, lowest_log_level: str, log_handlers: Optional[List[Handler]] = None):
        self._logger = logging.getLogger(log_source)
        self._logger.setLevel(lowest_log_level)

        if not log_handlers or len(log_handlers) == 0:
            log_handlers = [StreamHandler(sys.stdout)]

        for log_handler in log_handlers:
            log_handler.setFormatter(json_log_formatter.JSONFormatter())
            self._logger.addHandler(log_handler)

    def log(self, data: LogEntry):
        if data.level == LogLevel.INFO:
            self._logger.info()