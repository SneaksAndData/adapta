"""
 Base class representing file system path.
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

from abc import abstractmethod, ABC
from enum import Enum


class DataProtocols(Enum):
    """
    HDFS protocol aliases for data path implementations.
    """

    AZURE_BLOB = "wasbs"
    ADLS2 = "abfss"
    FILE = "file"
    HIVE = "hive"
    ASTRA = "astra"
    S3 = "s3"


class DataPath(ABC):
    """
    Base path to any data entity.
    """

    path: str
    protocol: str

    @classmethod
    @abstractmethod
    def from_hdfs_path(cls, hdfs_path: str) -> "DataPath":
        """
          Converts HDFS FileSystem path notation to this class.
        :param hdfs_path: abfss://...
        :return:
        """

    @classmethod
    @abstractmethod
    def from_uri(cls, url: str) -> "DataPath":
        """
          Converts URL  to this class.
        :param url: https://...
        :return:
        """

    @abstractmethod
    def to_uri(self) -> str:
        """
          Returns valid URL from this class.
        :return:
        """

    @abstractmethod
    def base_uri(self) -> str:
        """
          Returns valid base service URL from this class.
        :return:
        """

    @abstractmethod
    def to_hdfs_path(self) -> str:
        """
         Returns valid HDFS path from this class.
        :return:
        """

    @abstractmethod
    def to_delta_rs_path(self) -> str:
        """
         Returns valid Delta-RS (https://github.com/delta-io/delta-rs) path from this class.
        :return:
        """
