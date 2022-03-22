from abc import abstractmethod, ABC


class DataPath(ABC):
    path: str

    @abstractmethod
    def from_hdfs_path(cls, hdfs_path: str) -> "DataPath":
        """
           Converts HDFS FileSystem path notation to this class.
         :param hdfs_path: abfss://...
         :return:
        """
        pass

    @abstractmethod
    def to_hdfs_path(self) -> str:
        """
         Returns valid HDFS path from this class.
        :return:
        """
        pass

    @abstractmethod
    def to_delta_rs_path(self) -> str:
        """
         Returns valid Delta-RS (https://github.com/delta-io/delta-rs) path from this class.
        :return:
        """
        pass
