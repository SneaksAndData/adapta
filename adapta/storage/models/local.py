"""
 Models used by Local Client when working with storage.
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

from adapta.storage.models.base import DataPath, DataProtocols


@dataclass
class LocalPath(DataPath):
    """
    Local file system path.
    """

    def base_uri(self) -> str:
        raise NotImplementedError

    @classmethod
    def from_uri(cls, url: str) -> "DataPath":
        return LocalPath.from_hdfs_path(url)

    def to_uri(self) -> str:
        return self.to_hdfs_path()

    path: str
    protocol: str = DataProtocols.FILE.value

    @classmethod
    def from_hdfs_path(cls, hdfs_path: str) -> "LocalPath":
        assert hdfs_path.startswith("file://"), "HDFS local path should start with file://"

        return LocalPath(path=hdfs_path.replace("file://", ""))

    def to_hdfs_path(self) -> str:
        return f"file://{self.path}"

    def to_delta_rs_path(self) -> str:
        return self.path
