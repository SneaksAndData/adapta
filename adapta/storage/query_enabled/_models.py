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
from typing import TypeVar, Generic, Type, Iterator, Union, final, Optional

import pandas

from adapta.storage.models.base import DataPath
from adapta.storage.models.filter_expression import Expression

TCredential = TypeVar("TCredential")
TSettings = TypeVar("TSettings")

# TODO: allow credential class as string or as a enum
CONNECTION_STRING_TEMPLATE = "qes://class={credential_class};plaintext_credentials={credentials};settings={settings}"
CONNECTION_STRING_REGEX = r"^qes:\/\/class=(.*?);plaintext_credentials=(.*?);settings=(.*?)$"


class QueryEnabledStore(Generic[TCredential, TSettings], ABC):
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

    def open(self, path: DataPath) -> "QueryEnabledStoreReader":
        return QueryEnabledStoreReader(self, path)

    @abstractmethod
    def _apply_filter(
        self, path: DataPath, filter_expression: Expression, columns: list[str]
    ) -> Union[pandas.DataFrame, Iterator[pandas.DataFrame]]:
        """
        Applies the provided filter expression to this Store and returns the result in a pandas DataFrame
        """

    @abstractmethod
    def _apply_query(self, query: str) -> Union[pandas.DataFrame, Iterator[pandas.DataFrame]]:
        """
        Applies a plaintext query to this Store and returns the result in a pandas DataFrame
        """

    @classmethod
    @abstractmethod
    def _from_connection_string(cls, connection_string: str) -> "QueryEnabledStore[TCredential, TSettings]":
        """
        Constructs the connection from a connection string
        """

    @staticmethod
    def from_string(connection_string: str) -> "QueryEnabledStore[TCredential, TSettings]":
        class_name, _, _ = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        class_object: Type[QueryEnabledStore[TCredential, TSettings]] = locate(class_name)
        return class_object._from_connection_string(connection_string)


@final
class QueryEnabledStoreReader:
    def __init__(self, store: QueryEnabledStore, path: DataPath):
        self._store = store
        self._path = path
        self._filter_expression: Optional[Expression] = None
        self._columns = []

    def filter(self, filter_expression: Expression) -> "QueryEnabledStoreReader":
        self._filter_expression = filter_expression
        return self

    def select(self, *columns: str) -> "QueryEnabledStoreReader":
        self._columns = columns
        return self

    def read(self):
        return self._store._apply_filter(
            path=self._path, filter_expression=self._filter_expression, columns=self._columns
        )
