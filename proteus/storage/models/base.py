"""
 Base class representing file system path.
"""
from abc import abstractmethod, ABC


class DataPath(ABC):
    """
     Base path to any data entity.
    """
    path: str

    @classmethod
    @abstractmethod
    def from_hdfs_path(cls, hdfs_path: str) -> "DataPath":
        """
           Converts HDFS FileSystem path notation to this class.
         :param hdfs_path: abfss://...
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
