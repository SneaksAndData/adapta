"""
  Models used for inter-process communication in data processing applications.
"""
import re
from dataclasses import dataclass
from typing import Optional, List, Iterable

from proteus.storage.models.base import DataPath
from proteus.storage.models.azure import AdlsGen2Path, WasbPath
from proteus.storage.models.local import LocalPath
from proteus.storage.models.hive import HivePath
from proteus.storage.models.format import SerializationFormat, DataFrameParquetSerializationFormat, \
    DataFrameCsvSerializationFormat, DictJsonSerializationFormat, DataFrameJsonSerializationFormat


@dataclass(frozen=True)
class DataSocket:
    """
      Defines an input or an output of a data processing application.
    """

    # name of a Socket
    alias: str

    # path to data (read-in, write-out)
    data_path: str

    # format of the data (read-in, write-out)
    data_format: str

    def __post_init__(self):
        assert self.alias and self.data_path and self.data_format, \
            'All fields must have a value provided to instantiate a DataSocket.'

    def parse_data_path(
            self,
            candidates: List[DataPath] = (AdlsGen2Path, LocalPath, HivePath, WasbPath)
    ) -> Optional[DataPath]:
        """
          Attempts to convert this socket's data path to one of the known DataPath types.

        :param candidates: Conversion candidates.

        :return:
        """
        for candidate in candidates:
            try:
                return candidate.from_hdfs_path(self.data_path)
            except:
                continue

        return None

    def parse_serialization_format(
            self,
            candidates: List[SerializationFormat] = (
                    DataFrameParquetSerializationFormat,
                    DataFrameCsvSerializationFormat,
                    DictJsonSerializationFormat,
                    DataFrameJsonSerializationFormat
            )
    ) -> Iterable[SerializationFormat]:
        """
         Tries to find one or more matching SerializationFormat for this socket.

        :param candidates: Candidate formats.
        :return:
        """
        for candidate in candidates:
            candidate_name = candidate.__name__[0].lower() + re.sub(
                r"[A-Z]",
                lambda matched: '_' + matched.group(0).lower(),
                candidate.__name__[1:]
            )

            if f"_{self.data_format}_" in candidate_name:
                yield candidate

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
