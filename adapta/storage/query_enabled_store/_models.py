"""
 Query Enabled Store Connection interface.
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

import re
from abc import ABC, abstractmethod
from enum import Enum
from pydoc import locate
from typing import TypeVar, Generic, Type, Iterator, Union, final, Optional

from adapta.storage.models.base import DataPath
from adapta.storage.models.filter_expression import Expression
from adapta.storage.models.enum import QueryEnabledStoreOptions
from adapta.utils.metaframe import MetaFrame

TCredential = TypeVar("TCredential")  # pylint: disable=C0103
TSettings = TypeVar("TSettings")  # pylint: disable=C0103

CONNECTION_STRING_REGEX = r"^qes:\/\/engine=(.*?);plaintext_credentials=(.*?);settings=(.*?)$"


@final
class BundledQueryEnabledStores(Enum):
    """
    QES Implementations aliases that are bundled with Adapta.
    """

    DELTA = "adapta.storage.query_enabled_store.DeltaQueryEnabledStore"
    ASTRA = "adapta.storage.query_enabled_store.AstraQueryEnabledStore"
    LOCAL = "adapta.storage.query_enabled_store.LocalQueryEnabledStore"


BUNDLED_STORES = {store.name: store.value for store in BundledQueryEnabledStores}


class QueryEnabledStore(Generic[TCredential, TSettings], ABC):
    """
    QES base class.
    """

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

    def open(self, path: DataPath) -> "QueryConfigurationBuilder":
        """
        Construct a reader object for QES to proxy to the underlying store implementation.
        """
        return QueryConfigurationBuilder(self, path)

    @abstractmethod
    def close(self) -> None:
        """
        Optional logic to dispose of the store connections and related resources.
        """

    @abstractmethod
    def _apply_filter(
        self,
        path: DataPath,
        filter_expression: Expression,
        columns: list[str],
        options: dict[QueryEnabledStoreOptions, any] | None = None,
        limit: Optional[int] = None,
    ) -> Union[MetaFrame, Iterator[MetaFrame]]:
        """
        Applies the provided filter expression to this Store and returns the result in a MetaFrame
        """

    @abstractmethod
    def _apply_query(self, query: str) -> Union[MetaFrame, Iterator[MetaFrame]]:
        """
        Applies a plaintext query to this Store and returns the result in a MetaFrame
        """

    @classmethod
    @abstractmethod
    def _from_connection_string(
        cls, connection_string: str, lazy_init: bool = False
    ) -> "QueryEnabledStore[TCredential, TSettings]":
        """
        Constructs the connection from a connection string

        :param: connection_string: QES connection string.
        :param: lazy_init: Whether to set this instance QES for querying eagerly or lazily.
        """

    @staticmethod
    def from_string(connection_string: str, lazy_init: bool = False) -> "QueryEnabledStore[TCredential, TSettings]":
        """
        Constructs a concrete QES instance from a connection string.

        :param: connection_string: QES connection string.
        :param: lazy_init: Whether to set this instance QES for querying eagerly or lazily.
        """

        def get_qes_class(name: str) -> Type[QueryEnabledStore[TCredential, TSettings]]:
            return locate(BUNDLED_STORES.get(name, name))

        class_name, _, _ = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        class_object = get_qes_class(class_name)
        if class_object is None:
            raise ModuleNotFoundError(
                f"Cannot locate QES implementation: {class_name}. Please check the name for spelling errors and make sure your application can resolve the import"
            )
        return class_object._from_connection_string(connection_string, lazy_init)


@final
class QueryConfigurationBuilder:
    """
    Builder-pattern support for querying via QES.
    """

    def __init__(self, store: QueryEnabledStore, path: DataPath):
        self._store = store
        self._path = path
        self._filter_expression: Optional[Expression] = None
        self._columns: list[str] = []
        self._options: dict[QueryEnabledStoreOptions, any] = {}
        self._limit = None

    def filter(self, filter_expression: Expression) -> "QueryConfigurationBuilder":
        """
        Use the provided expression when querying the underlying storage.
        """
        self._filter_expression = (
            filter_expression if self._filter_expression is None else self._filter_expression and filter_expression
        )
        return self

    def select(self, *columns: str) -> "QueryConfigurationBuilder":
        """
        Request the underlying store to project the result onto the provided column set.
        """
        self._columns = list(columns)
        return self

    def add_options(self, option_key: QueryEnabledStoreOptions, option_value: any) -> "QueryConfigurationBuilder":
        """
        Use the provided options when querying the underlying storage.
        """

        self._options[option_key] = option_value
        return self

    def limit(self, limit: int | None) -> "QueryConfigurationBuilder":
        """
        Limit the number of results returned by the underlying store.
        """
        self._limit = limit
        return self

    def read(self) -> Union[MetaFrame, Iterator[MetaFrame]]:
        """
        Execute the query on the underlying store.
        """
        return self._store._apply_filter(
            path=self._path,
            filter_expression=self._filter_expression,
            columns=self._columns,
            options=self._options,
            limit=self._limit,
        )
