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
from pydoc import locate
from typing import Self

from adapta.storage.distributed_object_store.v3.cassandra_client import TCassandraModel
from adapta.storage.models.base import DataPath, DataProtocols


@dataclass
class CassandraPath(DataPath):
    """
    Path wrapper for Astra DB.
    """

    def to_uri(self) -> str:
        return self.to_hdfs_path()

    def base_uri(self) -> str:
        raise NotImplementedError

    @classmethod
    def from_uri(cls, url: str) -> Self:
        raise NotImplementedError

    @classmethod
    def _path_regex(cls) -> str:
        return r"^(cass)(?:\+(.+))?:\/\/(.+)@(.+)$"

    keyspace: str
    table: str
    model_class_name: str | None = None
    protocol: str = DataProtocols.CASSANDRA.value

    @classmethod
    def from_hdfs_path(cls, hdfs_path: str) -> Self:
        match = re.match(cls._path_regex(), hdfs_path)
        assert match, f"Invalid path provided, must comply with: {cls._path_regex()}"
        return cls(keyspace=match.group(3), table=match.group(4), model_class_name=match.group(2))

    def to_hdfs_path(self) -> str:
        if not self.model_class_name:
            return f"${self.protocol}://{self.keyspace}@{self.table}"
        return f"${self.protocol}+{self.model_class_name}://{self.keyspace}@{self.table}"

    def to_delta_rs_path(self) -> str:
        raise NotImplementedError

    def model_class(self) -> type[TCassandraModel] | None:
        """
        Locates and returns model class name.
        """
        if self.model_class_name:
            return locate(self.model_class_name)

        return None
