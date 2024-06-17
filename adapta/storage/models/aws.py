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
        Converts the S3Path to a URI.
         :return: URI path
        """
        if not self.bucket or not self.path:
            raise ValueError("Bucket and path must be defined")

        return f"s3://{self.bucket}/{self.path}"

    def base_uri(self) -> str:
        """
        Returns the base URI of the S3Path.
        :return: URI path
        """
        if not self.bucket:
            raise ValueError("Bucket must be defined")

        return f"https://{self.bucket}.s3.amazonaws.com"

    @classmethod
    def from_uri(cls, url: str) -> "S3Path":
        """
        Creates an S3Path from a URI.
        :return: S3Path path
        """
        assert url.startswith(("http://", "https://"))
        uri = urlparse(url)
        return cls(bucket=uri.netloc, path=uri.path.lstrip("/"))

    bucket: str
    path: str
    protocol: str = DataProtocols.S3.value

    @classmethod
    def from_hdfs_path(cls, hdfs_path: str) -> "S3Path":
        """
        Converts the HDFS path to S3Path compatible path.
        :return: S3Path path
        """
        assert hdfs_path.startswith("s3a://"), "HDFS S3 path should start with s3a://"
        uri = urlparse(hdfs_path)
        parsed_path = uri.path.split("/")
        return cls(bucket=uri.netloc, path="/".join(parsed_path[1:]))

    def to_hdfs_path(self) -> str:
        """
        Converts the S3Path to an HDFS compatible path.
        :return: HDFS path
        """
        if not self.bucket or not self.path:
            raise ValueError("Bucket and path must be defined")

        return f"s3a://{self.bucket}/{self.path}"

    def to_delta_rs_path(self) -> str:
        """
        Converts the S3Path to a Delta Lake compatible path.
        :return: Delta Lake path
        """
        return f"s3a://{self.bucket}/{self.path}"


def cast_path(blob_path: DataPath) -> S3Path:
    """
     Type cast from DataPath to S3Path

    :param blob_path: DataPath
    :return: S3Path
    """
    assert isinstance(blob_path, S3Path), "Only S3 paths are supported by this client."

    return blob_path
