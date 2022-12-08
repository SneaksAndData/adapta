"""
  Models used for inter-process communication in data processing applications.
"""
import re
from dataclasses import dataclass
from typing import Optional, List, Iterable

from dataclasses_json import DataClassJsonMixin

from proteus.storage.models.base import DataPath
from proteus.storage.models.azure import AdlsGen2Path, WasbPath
from proteus.storage.models.local import LocalPath
from proteus.storage.models.hive import HivePath
from proteus.storage.models.format import SerializationFormat, DataFrameParquetSerializationFormat, \
    DataFrameCsvSerializationFormat, DictJsonSerializationFormat, DataFrameJsonSerializationFormat


@dataclass(frozen=True)
class DataSocket(DataClassJsonMixin):
    """
      Defines an input or an output of a data processing application.
    """

    # name of a Socket
    alias: str

    # path to data (read-in, write-out)
    data_path: str

    # format of the data (read-in, write-out)
    data_format: str

    # optional partitions that exist in the data (read-in, write-out)
    data_partitions: Optional[List[str]] = None

    def __post_init__(self):
        assert self.alias and self.data_path and self.data_format, \
            'Fields alias, data_path and data_format must have a value provided to instantiate a DataSocket.'

    def parse_data_path(
            self,
            candidates: Iterable[DataPath] = (AdlsGen2Path, LocalPath, HivePath, WasbPath)
    ) -> Optional[DataPath]:
        """
          Attempts to convert this socket's data path to one of the known DataPath types.

        :param candidates: Conversion candidates.

        :return:
        """
        for candidate in candidates:
            try:
                return candidate.from_hdfs_path(self.data_path)
            except:  # pylint: disable=W0702
                continue

        return None

    def serialize(self) -> str:
        """
         Serializes to a |-delimited string
        """
        return f"{self.alias}|{self.data_path}|{self.data_format}"

    @classmethod
    def deserialize(cls, string_socket: str) -> 'DataSocket':
        """
          Deserializes from a |-delimited string
        """
        vals = string_socket.split('|')
        return cls(alias=vals[0], data_path=vals[1], data_format=vals[2])

    @staticmethod
    def find(sockets: List['DataSocket'], alias: str) -> 'DataSocket':
        """Fetches a job socket from list of sockets.
        :param sockets: List of sockets
        :param alias: Alias to look up

        :returns: Socket with alias 'alias'
        """
        socket = [s for s in sockets if s.alias == alias]

        if len(socket) > 1:
            raise ValueError(f'Multiple job sockets exist with alias {alias}')
        if len(socket) == 0:
            raise ValueError(f'No job sockets exist with alias {alias}')
        return socket[0]
