"""
 Models used by Azure Client when working with storage.
"""
from dataclasses import dataclass
from typing import Union

from proteus.storage.models.base import DataPath, DataProtocols


@dataclass
class AdlsGen2Path(DataPath):
    """
    Path wrapper for ADLS Gen2.
    """

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
            path=hdfs_path.split(".dfs.core.windows.net")[1][1:],
        )

    def _check_path(self):
        assert not self.path.startswith("/"), "Path should not start with /"

    def to_hdfs_path(self) -> str:
        self._check_path()
        return f"wasbs://{self.container}@{self.account}.blob.core.windows.net/{self.path}"

    def to_delta_rs_path(self) -> str:
        raise NotImplementedError("WASB not supported by delta-rs yet")


def cast_path(blob_path: DataPath) -> Union[AdlsGen2Path, WasbPath]:
    """
     Type cast from DataPath to one of Azure paths.

    :param blob_path: DataPath
    :return: AdlsGen2Path or WasbPath
    """
    assert isinstance(blob_path, (AdlsGen2Path, WasbPath)), "Only Azure Data paths are supported by this client."

    return blob_path
