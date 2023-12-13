"""
 Query Enabled Store Connection interface.
"""

#  Copyright (c) 2023. ECCO Sneaks & Data
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

import re
from abc import ABC, abstractmethod
from pydoc import locate
from typing import TypeVar, Generic, Type

TCredential = TypeVar("TCredential")
TSettings = TypeVar("TSettings")

# TODO: hide class name behind enums in sqlalchemy format: qes+<class alias>
CONNECTION_STRING_TEMPLATE = "qes://class={credential_class};plaintext_credentials={credentials};settings={settings}"
CONNECTION_STRING_REGEX = r"^qes:\/\/class=(.*?);plaintext_credentials=(.*?);settings=(.*?)$"


class QueryEnabledStoreConnection(Generic[TCredential, TSettings], ABC):
    def __init__(self, credentials: TCredential, settings: TSettings):
        self._credentials = credentials
        self._settings = settings

    @property
    def credentials(self) -> TCredential:
        """
        Returns the credentials for this store type.
        """
        return self._credentials

    @property
    def settings(self) -> TSettings:
        """
        Returns the address to connect to, if applicable.
        """
        return self._settings

    @classmethod
    @abstractmethod
    def _from_connection_string(cls, connection_string: str) -> "QueryEnabledStoreConnection[TCredential, TSettings]":
        """
        Constructs the connection from a connection string
        """

    @staticmethod
    def from_string(connection_string: str) -> "QueryEnabledStoreConnection[TCredential, TSettings]":
        class_name, _, _ = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        class_object: Type[QueryEnabledStoreConnection[TCredential, TSettings]] = locate(class_name)
        return class_object._from_connection_string(connection_string)
