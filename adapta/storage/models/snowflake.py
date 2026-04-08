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
import re
from dataclasses import dataclass
from typing import TypeVar

from adapta.storage.models.base import DataPath, DataProtocols

TModel = TypeVar("TModel")  # pylint: disable=C0103


@dataclass
class SnowflakePath(DataPath):
    """
    Path wrapper for Snowflake.
    """

    database: str
    schema: str
    table: str
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
        match = re.match(r"^snowflake://([^/]+)/([^/]+)/([^/]+)$", hdfs_path)
        assert match, f"Invalid Snowflake path: {hdfs_path}"
        return cls(database=match.group(1), schema=match.group(2), table=match.group(3))

    def to_hdfs_path(self) -> str:
        return f"snowflake://{self.database}/{self.schema}/{self.table}"

    @property
    def fully_qualified_name(self) -> str:
        """Combine database, schema and table into fully qualified name"""
        return f'"{self.database}"."{self.schema}"."{self.table}"'

    def to_delta_rs_path(self) -> str:
        raise NotImplementedError
