"""
  Models used for inter-process communication in data processing applications.
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
from typing import Optional, List, Iterable

from dataclasses_json import DataClassJsonMixin

from adapta.storage.models import parse_data_path
from adapta.storage.models.astra import AstraPath
from adapta.storage.models.base import DataPath
from adapta.storage.models.azure import AdlsGen2Path, WasbPath
from adapta.storage.models.local import LocalPath


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
        assert (
            self.alias and self.data_path and self.data_format
        ), "Fields alias, data_path and data_format must have a value provided to instantiate a DataSocket."

    def parse_data_path(
        self, candidates: Iterable[DataPath] = (AdlsGen2Path, LocalPath, WasbPath, AstraPath)
    ) -> Optional[DataPath]:
        """
          Attempts to convert this socket's data path to one of the known DataPath types.

        :param candidates: Conversion candidate classes for `DataPath`. Default to all currently supported `DataPath` implementations.
          If a user has their own `DataPath` implementations, those can be supplied instead for convenience.

        :return:
        """
        return parse_data_path(self.data_path, candidates=candidates)

    def serialize(self) -> str:
        """
        Serializes to a |-delimited string
        """
        return f"{self.alias}|{self.data_path}|{self.data_format}"

    @classmethod
    def deserialize(cls, string_socket: str) -> "DataSocket":
        """
        Deserializes from a |-delimited string
        """
        vals = string_socket.split("|")
        return cls(alias=vals[0], data_path=vals[1], data_format=vals[2])

    @staticmethod
    def find(sockets: List["DataSocket"], alias: str) -> "DataSocket":
        """Fetches a data socket from a list of sockets.
        :param sockets: List of sockets
        :param alias: Alias to look up

        :returns: Socket with alias 'alias'
        """
        socket = [s for s in sockets if s.alias == alias]

        if len(socket) > 1:
            raise ValueError(f"Multiple data sockets exist with alias {alias}")
        if len(socket) == 0:
            raise ValueError(f"No data sockets exist with alias {alias}")
        return socket[0]
