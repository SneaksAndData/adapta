"""
 Storage enums.
"""

from enum import Enum


class QueryEnabledStoreOptions(Enum):
    """
    QES options aliases in Adapta.

    If CONCAT_OPTIONS is used, list[MetaFrameOptions] is expected.
    If ALLOW_PARTITIONING_FILTERING is used, bool is expected.
    """

    CONCAT_OPTIONS = "concat_options"
    ALLOW_PARTITIONING_FILTERING = "allow_partitioning_filtering"
