"""
 Models used by Astra DB when working with storage.
"""
#  Copyright (c) 2023-2024. ECCO Sneaks & Data
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
from pydoc import locate
from typing import Optional, TypeVar, Type
from urllib.parse import urlparse

from adapta.storage.models.base import DataPath, DataProtocols

TModel = TypeVar("TModel")  # pylint: disable=C0103


@dataclass
class AstraPath(DataPath):
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

    keyspace: str
    table: str
    model_class_name: Optional[str] = None
    protocol: str = DataProtocols.ASTRA.value

    @classmethod
    def from_hdfs_path(cls, hdfs_path: str) -> "AstraPath":
        assert hdfs_path.startswith("astra://"), "HDFS astra path should start with astra://"
        parsed_path = urlparse(hdfs_path).netloc.split("@")
        return cls(keyspace=parsed_path[0], table=parsed_path[1])

    def to_hdfs_path(self) -> str:
        return f"astra://{self.keyspace}@{self.table}"

    def to_delta_rs_path(self) -> str:
        raise NotImplementedError

    def model_class(self) -> Optional[Type[TModel]]:
        """
        Locates and returns model class name.
        """
        if self.model_class_name:
            return locate(self.model_class_name)

        return None
