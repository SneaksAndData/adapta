"""
 Models used by Azure Client when working with storage.
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
from typing import Union

from adapta.storage.models.base import DataPath, DataProtocols


@dataclass
class AdlsGen2Path(DataPath):
    """
    Path wrapper for ADLS Gen2.
    """

    def base_uri(self) -> str:
        return f"https://{self.account}.dfs.core.windows.net"

    @classmethod
    def from_uri(cls, url: str) -> "DataPath":
        assert url.startswith("https://") and (
            "dfs.core.windows.net" in url
        ), "Invalid URL supplied. Please use the following format: https://<accountname>.dfs.core.windows.net or https://<accountname>.blob.core.windows.net"

        return cls(
            account=url.split("://")[1].split(".")[0],
            container=url.split(".windows.net/")[1].split("/")[0],
            path="/".join(url.split(".windows.net/")[1].split("/")[1:]),
        )

    def to_uri(self) -> str:
        return f"https://{self.account}.dfs.core.windows.net/{self.container}/{self.path}"

    account: str
    container: str
    path: str
    protocol: str = DataProtocols.ADLS2.value

    @classmethod
    def from_hdfs_path(cls, hdfs_path: str) -> "AdlsGen2Path":
        assert (
            "@" in hdfs_path and "dfs.core.windows.net" in hdfs_path and hdfs_path.startswith("abfss://")
        ), "Invalid HDFS (ALDS2) path supplied. Please use the following format: abfss://<container>@<account>.dfs.core.windows.net/my/data"

        return AdlsGen2Path(
            account=hdfs_path.split("@")[1].split(".")[0],
            container=hdfs_path.split("@")[0].split("//")[1],
            path=hdfs_path.split(".dfs.core.windows.net")[1][1:],
        )

    def _check_path(self):
        assert not self.path.startswith("/"), "Path should not start with /"

    def to_hdfs_path(self) -> str:
        self._check_path()
        return f"abfss://{self.container}@{self.account}.dfs.core.windows.net/{self.path}"

    def to_delta_rs_path(self) -> str:
        self._check_path()
        return f"az://{self.container}/{self.path}"


@dataclass
class WasbPath(DataPath):
    """
    Path wrapper for ADLS Gen2.
    """

    def base_uri(self) -> str:
        return f"https://{self.account}.blob.core.windows.net"

    @classmethod
    def from_uri(cls, url: str) -> "DataPath":
        assert url.startswith("https://") and (
            "blob.core.windows.net" in url
        ), "Invalid URL supplied. Please use the following format: https://<accountname>.blob.core.windows.net"

        return cls(
            account=url.split("://")[1].split(".")[0],
            container=url.split(".windows.net/")[1].split("/")[0],
            path="/".join(url.split(".windows.net/")[1].split("/")[1:]),
        )

    def to_uri(self) -> str:
        return f"https://{self.account}.blob.core.windows.net/{self.container}/{self.path}"

    account: str
    container: str
    path: str
    protocol: str = DataProtocols.AZURE_BLOB.value

    @classmethod
    def from_hdfs_path(cls, hdfs_path: str) -> "WasbPath":
        assert (
            "@" in hdfs_path and "blob.core.windows.net" in hdfs_path and hdfs_path.startswith("wasbs://")
        ), "Invalid HDFS (WASB) path supplied. Please use the following format: wasbs://<container>@<account>.blob.core.windows.net/my/data"

        return WasbPath(
            account=hdfs_path.split("@")[1].split(".")[0],
            container=hdfs_path.split("@")[0].split("//")[1],
            path=hdfs_path.split(".blob.core.windows.net")[1][1:],
        )

    def _check_path(self):
        assert not self.path.startswith("/"), "Path should not start with /"

    def to_hdfs_path(self) -> str:
        self._check_path()
        return f"wasbs://{self.container}@{self.account}.blob.core.windows.net/{self.path}"

    def to_delta_rs_path(self) -> str:
        raise NotImplementedError("WASB not supported by delta-rs yet")

    @classmethod
    def from_adls2_path(cls, adls_path: AdlsGen2Path) -> "WasbPath":
        """
        Convenience method for converting abfss path to wasbs path. This has some use cases when you need to generate account url, which is different from DFS endpoint,
        and most client use Blob endpoint for ADLS2 accounts.
        """

        return cls(account=adls_path.account, container=adls_path.container, path=adls_path.path)


def cast_path(blob_path: DataPath) -> Union[AdlsGen2Path, WasbPath]:
    """
     Type cast from DataPath to one of Azure paths.

    :param blob_path: DataPath
    :return: AdlsGen2Path or WasbPath
    """
    assert isinstance(blob_path, (AdlsGen2Path, WasbPath)), "Only Azure Data paths are supported by this client."

    return blob_path
