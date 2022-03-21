"""
 Models used by storage clients.
"""
from dataclasses import dataclass

from proteus.storage.models.base import DataPath


@dataclass
class AdlsGen2Path(DataPath):
    """
     Path wrapper for ADLS Gen2.
    """
    account: str
    container: str
    path: str

    @classmethod
    def from_hdfs_path(cls, hdfs_path: str) -> "AdlsGen2Path":
        assert '@' in hdfs_path and 'dfs.core.windows.net' in hdfs_path and hdfs_path.startswith(
            'abfss://'), 'Invalid HDFS (ALDS2) path supplied. Please use the following format: abfss://<container>@<account>.dfs.core.windows.net/my/data'

        return AdlsGen2Path(
            account=hdfs_path.split('@')[1].split('.')[0],
            container=hdfs_path.split('@')[0].split('//')[1],
            path=hdfs_path.split('.dfs.core.windows.net')[1][1:]
        )

    def _check_path(self):
        assert not self.path.startswith('/'), 'Path should not start with /'

    def to_hdfs_path(self) -> str:
        self._check_path()
        return f"abfss://{self.container}@{self.account}.dfs.core.windows.net/{self.path}"

    def to_delta_rs_path(self) -> str:
        self._check_path()
        return f"adls2://{self.account}/{self.container}/{self.path}"
