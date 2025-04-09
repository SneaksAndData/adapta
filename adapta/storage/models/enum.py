"""
 Storage enums.
"""

from enum import Enum


class QueryEnabledStoreOptions(Enum):
    """
    QES options aliases in Adapta.

    If CONCAT_OPTIONS is used, list[MetaFrameOptions] is expected.
    If TIMEOUT is used, int is expected (time measured in seconds).
    """

    CONCAT_OPTIONS = "concat_options"
    TIMEOUT = "timeout"
