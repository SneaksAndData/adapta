"""
 Storage enums.
"""

from enum import Enum


class QueryEnabledStoreOptions(Enum):
    """
    QES options aliases in Adapta.

    If CONCAT_OPTIONS is used, list[MetaFrameOptions] is expected.
    """

    CONCAT_OPTIONS = "concat_options"
