"""
 Models used by Local Client when working with storage.
"""
from dataclasses import dataclass

from proteus.storage.models.base import DataPath


@dataclass
class LocalPath(DataPath):
    path: str

    @classmethod
    def from_hdfs_path(cls, hdfs_path: str) -> "LocalPath":
        assert hdfs_path.startswith("file://"), "HDFS local path should start with file://"

        return LocalPath(
            path=hdfs_path.replace("file://", "")
        )

    def to_hdfs_path(self) -> str:
        return f"file://{self.path}"

    def to_delta_rs_path(self) -> str:
        return self.path
