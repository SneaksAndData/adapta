"""
 Base class representing file system path.
"""
from abc import abstractmethod, ABC
from enum import Enum


class DataProtocols(Enum):
    """
     HDFS protocol aliases for data path implementations.
    """
    AZURE_BLOB = 'wasbs'
    ADLS2 = 'abfss'
    FILE = 'file'
    HIVE = 'hive'


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
    def from_url(cls, url: str) -> "DataPath":
        """
           Converts URL  to this class.
         :param url: https://...
         :return:
        """

    @abstractmethod
    def to_url(self) -> "DataPath":
        """
           Returns valid URL from this class.
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
