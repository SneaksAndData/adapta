"""
 Query Enabled Store Connection interface.
"""

#  Copyright (c) 2023-2026. ECCO Data & AI and other project contributors.
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
from collections.abc import Iterator
from enum import Enum
from functools import partial
from pydoc import locate
from typing import TypeVar, Generic, final, Self, Callable

from adapta.storage.models.base import DataPath
from adapta.storage.models.enum import QueryEnabledStoreOptions
from adapta.storage.models.expression_dsl.filter_expression import Expression
from adapta.storage.query_enabled_store.parameters import (
    QueryEnabledStoreDataParameter,
    QueryEnabledStoreOverwriteParameter,
    QueryEnabledStoreBlockSizeParameter,
    QueryEnabledStoreOperationParameter,
    QueryEnabledStoreFilterParameter,
    QueryEnabledStoreSelectParameter,
    QueryEnabledStoreReadOptionsParameter,
    QueryEnabledStoreLimitParameter,
)
from adapta.utils.metaframe import MetaFrame

TCredential = TypeVar("TCredential")  # pylint: disable=C0103
TSettings = TypeVar("TSettings")  # pylint: disable=C0103

CONNECTION_STRING_REGEX = r"^qes:\/\/engine=(.*?);plaintext_credentials=(.*?);settings=(.*?)$"


@final
class QueryEnabledStoreMode(Enum):
    """
    Defines data access mode for QES.
    """

    READ = "read"
    WRITE = ("write",)


@final
class BundledQueryEnabledStores(Enum):
    """
    QES Implementations aliases that are bundled with Adapta.
    """

    DELTA = "adapta.storage.query_enabled_store.DeltaQueryEnabledStore"
    ASTRA = "adapta.storage.query_enabled_store.AstraQueryEnabledStore"
    LOCAL = "adapta.storage.query_enabled_store.LocalQueryEnabledStore"
    TRINO = "adapta.storage.query_enabled_store.TrinoQueryEnabledStore"
    ICEBERG = "adapta.storage.query_enabled_store.IcebergQueryEnabledStore"


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

    def open(self, path: DataPath, access_mode: QueryEnabledStoreMode) -> "QueryEnabledStoreOperationBuilder":
        """
        Construct a reader object for QES to proxy to the underlying store implementation.
        """
        if access_mode == QueryEnabledStoreMode.READ:
            return _QueryEnabledStoreReadBuilder.create(self, path)
        if access_mode == QueryEnabledStoreMode.WRITE:
            return _QueryEnabledStoreWriteBuilder.create(self, path)

        raise NotImplementedError(f"Unsupported access mode {access_mode.value}")

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
        limit: int | None = None,
    ) -> MetaFrame | Iterator[MetaFrame]:
        """
        Applies the provided filter expression to this Store and returns the result in a MetaFrame
        """

    @abstractmethod
    def _apply_query(self, query: str) -> MetaFrame | Iterator[MetaFrame]:
        """
        Applies a plaintext query to this Store and returns the result in a MetaFrame
        """

    @abstractmethod
    def _write(self, path: DataPath, data: MetaFrame | Iterator[MetaFrame], block_size: int, overwrite: bool) -> None:
        """
        Writes `data` to the provided path, using the underlying store implementation.
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

        def get_qes_class(name: str) -> type[QueryEnabledStore[TCredential, TSettings]]:
            return locate(BUNDLED_STORES.get(name, name))

        class_name, _, _ = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        class_object = get_qes_class(class_name)
        if class_object is None:
            raise ModuleNotFoundError(
                f"Cannot locate QES implementation: {class_name}. Please check the name for spelling errors and make sure your application can resolve the import"
            )
        return class_object._from_connection_string(connection_string, lazy_init)


class QueryEnabledStoreOperationBuilder(ABC):
    """
    Base class for QES operation builders.
    """

    def __init__(self, store: QueryEnabledStore, path: DataPath):
        self._store = store
        self._path = path
        self._operation_parameters: dict[str, QueryEnabledStoreOperationParameter] = self._set_accepted_parameters()

    @abstractmethod
    def _set_accepted_parameters(self) -> dict[str, QueryEnabledStoreOperationParameter]:
        """
        Define parameters supported by this builder.
        """

    @abstractmethod
    def _operation_callable(self) -> Callable:
        """
        Operation to map into `execute` with `_operation_parameters`.
        """

    def set_parameter(self, parameter: QueryEnabledStoreOperationParameter) -> Self:
        """
        Set or update the provided parameter.
        """
        if parameter.name in self._operation_parameters:
            self._operation_parameters[parameter.name] = parameter
        else:
            raise ValueError(f"Parameter {parameter.name} is not supported by this builder.")

        return self

    def set_parameters(self, *parameters: QueryEnabledStoreOperationParameter) -> Self:
        """
        Set or update the provided parameters.
        """
        for parameter in parameters:
            self.set_parameter(parameter)

        return self

    def execute(self):
        """
        Build and execute the operation.
        """
        return partial(
            self._operation_callable,
            **{parameter.name: parameter.value for _, parameter in self._operation_parameters.items()},
        )

    @classmethod
    def create(cls, store: QueryEnabledStore, path: DataPath) -> Self:
        """
        Create an instance of `QueryEnabledStoreOperationBuilder`.
        """
        return cls(store, path)


@final
class _QueryEnabledStoreReadBuilder(QueryEnabledStoreOperationBuilder):
    def _set_accepted_parameters(self) -> dict[str, QueryEnabledStoreOperationParameter]:
        return {
            parameter.name: parameter
            for parameter in [
                QueryEnabledStoreFilterParameter(None),
                QueryEnabledStoreSelectParameter([]),
                QueryEnabledStoreReadOptionsParameter({}),
                QueryEnabledStoreLimitParameter(None),
            ]
        }

    def _operation_callable(self) -> Callable:
        return self._store._apply_filter


@final
class _QueryEnabledStoreWriteBuilder(QueryEnabledStoreOperationBuilder):
    def _set_accepted_parameters(self) -> dict[str, QueryEnabledStoreOperationParameter]:
        return {
            parameter.name: parameter
            for parameter in [
                QueryEnabledStoreDataParameter(None),
                QueryEnabledStoreOverwriteParameter(True),
                QueryEnabledStoreBlockSizeParameter(50_000),
            ]
        }

    def _operation_callable(self) -> Callable:
        return self._store._write
