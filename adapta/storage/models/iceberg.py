"""
 Models used by Astra DB when working with storage.
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
class IcebergPath(DataPath):
    """
    Path wrapper for Astra DB.
    """

    def to_uri(self) -> str:
        return self.to_hdfs_path()

    def base_uri(self) -> str:
        raise NotImplementedError

    @classmethod
    def from_uri(cls, url: str) -> "DataPath":
        raise NotImplementedError

    schema: str
    table: str
    protocol: str = DataProtocols.ICEBERG_REST.value

    @classmethod
    def from_hdfs_path(cls, hdfs_path: str) -> "IcebergPath":
        path_regex = r"^iceberg:\/\/([^@]+)@(.+)$"
        match = re.match(path_regex, hdfs_path)
        assert match, f"Invalid path provided, must comply with: {path_regex}"
        return cls(schema=match.group(1), table=match.group(2))

    def to_hdfs_path(self) -> str:
        return f"iceberg://{self.schema}@{self.table}"

    def to_delta_rs_path(self) -> str:
        raise NotImplementedError
