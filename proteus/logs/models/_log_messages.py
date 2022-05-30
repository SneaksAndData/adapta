"""
  Log message wrappers.
"""

import dataclasses
import json
from abc import ABC
from dataclasses import dataclass
from typing import Optional, Dict, Tuple

from proteus.logs.models._log_level import LogLevel


@dataclass
class BaseLog(ABC):
    """
     Base log entry. Field set changes depending on severity level.
    """
    template: str
    args: Dict
    level: str
    tags: Optional[str] = None

    def get_message(self, exclude_fields: Optional[Tuple[str]] = ('level', 'args')) -> str:
        """
         Returns message dictionary to be used by handler formatter.
         :param exclude_fields: Fields to exclude from export

        :return:
        """
        base_object = {k: v if type(v) == str else str(v) for k, v in dataclasses.asdict(self).items() if
                       v and k not in exclude_fields}
        base_object.setdefault('text', self.template.format(**self.args))

        return json.dumps(base_object)


@dataclass
class InfoLog(BaseLog):
    """
     Info entry.
    """
    level: LogLevel = dataclasses.field(default=LogLevel.INFO.value)


@dataclass
class WarnLog(BaseLog):
    """
      Warn entry.
    """
    exception: Optional[Exception] = None
    level: LogLevel = dataclasses.field(default=LogLevel.WARN.value)


@dataclass
class ErrorLog(BaseLog):
    """
     Error entry.
    """
    exception: Optional[Exception] = None
    level: LogLevel = dataclasses.field(default=LogLevel.ERROR.value)


@dataclass
class DebugLog(BaseLog):
    """
     Debug entry.
    """
    diagnostics: Optional[str] = None
    exception: Optional[Exception] = None
    level: LogLevel = dataclasses.field(default=LogLevel.DEBUG.value)
