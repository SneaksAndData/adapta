"""
 Models used by AWS when working with storage.
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
from urllib.parse import urlparse

from adapta.storage.models.base import DataPath, DataProtocols


@dataclass
class S3Path(DataPath):
    """
    Path wrapper for Astra DB.
    """

    def to_uri(self) -> str:
        """
        Not yet implemented in S3Path
        """
        raise NotImplementedError

    def base_uri(self) -> str:
        """
        Not yet implemented in S3Path
        """
        raise NotImplementedError

    @classmethod
    def from_uri(cls, url: str) -> "DataPath":
        """
        Not yet implemented in S3Path
        """
        raise NotImplementedError

    bucket: str
    path: str
    protocol: str = DataProtocols.S3.value

    @classmethod
    def from_hdfs_path(cls, hdfs_path: str) -> "S3Path":
        """
        Not yet implemented in S3Path
        """
        assert hdfs_path.startswith("s3a://"), "HDFS S3 path should start with s3a://"
        uri = urlparse(hdfs_path)
        parsed_path = uri.path.split("/")
        return cls(bucket=uri.netloc, path="/".join(parsed_path[1:]))

    def to_hdfs_path(self) -> str:
        """
        Not yet implemented in S3Path
        """
        return f"s3a://{self.bucket}/{self.path}"

    def to_delta_rs_path(self) -> str:
        """
        Not yet implemented in S3Path
        """
        raise NotImplementedError


def cast_path(blob_path: DataPath) -> S3Path:
    """
     Type cast from DataPath to S3Path

    :param blob_path: DataPath
    :return: S3Path
    """
    assert isinstance(blob_path, S3Path), "Only S3 paths are supported by this client."

    return blob_path
