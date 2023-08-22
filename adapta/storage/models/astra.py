"""
 Models used by Astra DB when working with storage.
"""
#  Copyright (c) 2023. ECCO Sneaks & Data
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

from adapta.storage.models.base import DataPath, DataProtocols


@dataclass
class AstraPath(DataPath):
    """
    Path wrapper for Astra DB.
    """

    def base_uri(self) -> str:
        raise NotImplementedError

    @classmethod
    def from_uri(cls, url: str) -> "DataPath":
        raise NotImplementedError

    keyspace: str
    table: str
    protocol: str = DataProtocols.ASTRA.value

    @classmethod
    def from_hdfs_path(cls, hdfs_path: str) -> "AstraPath":
        assert hdfs_path.startswith("astra://"), "HDFS astra path should start with astra://"
        return AstraPath(keyspace=hdfs_path.split("//")[1].split("@")[0], table=hdfs_path.split("@")[1])

    def _check_path(self):
        assert not self.path.startswith("/"), "Path should not start with /"

    def to_hdfs_path(self) -> str:
        return f"astra://{self.keyspace}@{self.table}"

    def to_delta_rs_path(self) -> str:
        raise NotImplementedError
