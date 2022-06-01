"""
 Standard log levels.
"""
from enum import Enum


class LogLevel(Enum):
    """
     Supported log levels.
    """
    INFO = 'INFO'
    WARN = 'WARN'
    ERROR = 'ERROR'
    DEBUG = 'DEBUG'
