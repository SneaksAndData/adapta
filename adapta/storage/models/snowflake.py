"""
 Models used by Snowflake when working with storage.
"""

#  Copyright (c) 2023-2026. ECCO Data & AI and other project contributors.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from dataclasses import dataclass
from typing import TypeVar

from adapta.storage.models.base import DataPath, DataProtocols

TModel = TypeVar("TModel")  # pylint: disable=C0103


@dataclass
class SnowflakePath(DataPath):
    """
    Path wrapper for Snowflake.
    """

    query: str
    protocol: str = DataProtocols.SNOWFLAKE.value

    def to_uri(self) -> str:
        raise NotImplementedError

    def base_uri(self) -> str:
        raise NotImplementedError

    @classmethod
    def from_uri(cls, url: str) -> "DataPath":
        raise NotImplementedError

    @classmethod
    def from_hdfs_path(cls, hdfs_path: str) -> "SnowflakePath":
        assert hdfs_path.startswith("snowflake://"), "HDFS Snowflake path should start with snowflake://"
        return cls(query=hdfs_path.removeprefix("snowflake://"))

    def to_hdfs_path(self) -> str:
        raise NotImplementedError

    def to_delta_rs_path(self) -> str:
        raise NotImplementedError
