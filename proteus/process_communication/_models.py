"""
  Models used for inter-process communication in data processing applications.
"""

from dataclasses import dataclass
from typing import Optional, List

from proteus.storage.models.base import DataPath
from proteus.storage.models.azure import AdlsGen2Path, WasbPath
from proteus.storage.models.local import LocalPath
from proteus.storage.models.hive import HivePath


@dataclass
class DataSocket:
    """
      Defines an input or an output of a data processing application.
    """

    # name of a Socket
    alias: str
    data_path: str
    data_format: str

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

    # maybe parse serialization format too

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
