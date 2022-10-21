"""
Models for log messages
"""

from dataclasses import dataclass
from typing import Optional, Dict


@dataclass
class ProteusLogMetadata:
    """
    Metadata model for log messages created by ProteusLogger
    """
    template: str
    diagnostics: Optional[str] = None
    tags: Optional[Dict[str, str]] = None
    fields: Dict[str, str] = None
