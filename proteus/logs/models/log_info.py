from dataclasses import dataclass
from typing import Optional

from proteus.logs.models.log_level import LogLevel


@dataclass
class LogEntry:
    level: LogLevel
    message_template: str
    message: str
    exception: Optional[Exception] = None
