"""
Models for log messages
"""

from dataclasses import dataclass
from types import TracebackType
from typing import Optional, Dict, Tuple


@dataclass
class ProteusLogMetadata:
    """
    Metadata model for log messages created by ProteusLogger
    """
    template: str
    diagnostics: Optional[str] = None
    tags: Optional[Dict[str, str]] = None
    fields: Optional[Dict[str, str]] = None
    exc_info: Optional[Tuple[type, BaseException, Optional[TracebackType]]] = None
