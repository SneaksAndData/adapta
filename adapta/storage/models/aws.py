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
from urllib.parse import urlparse

from adapta.storage.models.base import DataPath, DataProtocols


@dataclass
class S3Path(DataPath):
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

    path: str
    bucket: str
    protocol: str = DataProtocols.S3.value

    @classmethod
    def from_hdfs_path(cls, hdfs_path: str) -> "S3Path":
        assert hdfs_path.startswith("s3://"), "HDFS astra path should start with s3://"
        parsed_path = urlparse(hdfs_path).path.split("/")
        return cls(bucket=parsed_path[0], path="/".join(parsed_path[1]))

    def to_hdfs_path(self) -> str:
        raise NotImplementedError

    def to_delta_rs_path(self) -> str:
        raise NotImplementedError


def cast_path(blob_path: DataPath) -> S3Path:
    """
     Type cast from DataPath to S3Path

    :param blob_path: DataPath
    :return: S3Path
    """
    assert isinstance(blob_path, S3Path), "Only Azure Data paths are supported by this client."

    return blob_path
