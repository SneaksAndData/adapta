"""
 QES implementations for DataStax Astra.
"""
import os
import re
from dataclasses import dataclass
from typing import final, Optional, Union, Iterator

from dataclasses_json import DataClassJsonMixin

from adapta._version import __version__
from adapta.storage.distributed_object_store.v3.datastax_astra import AstraClient
from adapta.storage.models.astra import AstraPath
from adapta.storage.models.base import DataPath
from adapta.storage.models.filter_expression import Expression

from adapta.storage.query_enabled_store._models import (
    QueryEnabledStore,
    CONNECTION_STRING_REGEX,
)
from adapta.storage.models.enum import QueryEnabledStoreOptions
from adapta.utils.metaframe import MetaFrame


@dataclass
class AstraCredential(DataClassJsonMixin):
    """
    Astra DB credential helper for QES.
    """

    secret_connection_bundle_bytes: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None

    def __post_init__(self):
        self.secret_connection_bundle_bytes = self.secret_connection_bundle_bytes or os.getenv(
            "PROTEUS__ASTRA_BUNDLE_BYTES"
        )
        self.client_id = self.client_id or os.getenv("PROTEUS__ASTRA_CLIENT_ID")
        self.client_secret = self.client_secret or os.getenv("PROTEUS__ASTRA_CLIENT_SECRET")

        if not all([self.secret_connection_bundle_bytes, self.client_id, self.client_secret]):
            raise RuntimeError(
                "Authentication information provided is insufficient. Please verify you are supplying bundle bytes, client id and secret either via connection string or via environment variables."
            )


@dataclass
class AstraSettings(DataClassJsonMixin):
    """
    Astra DB connection settings for QES.
    """

    client_name: Optional[str] = None
    keyspace: Optional[str] = None

    def __post_init__(self):
        self.client_name = self.client_name or f"Adapta Client {__version__}"
        self.keyspace = self.keyspace or os.getenv("PROTEUS__ASTRA_KEYSPACE")


@final
class AstraQueryEnabledStore(QueryEnabledStore[AstraCredential, AstraSettings]):
    """
    QES Client for Astra DB (Cassandra).
    """

    def close(self) -> None:
        if not self._lazy:
            self._astra_client.disconnect()

    def __init__(self, credentials: AstraCredential, settings: AstraSettings, lazy_init: bool):
        super().__init__(credentials, settings)
        self._astra_client = AstraClient(
            client_name=self.settings.client_name,
            secure_connect_bundle_bytes=self.credentials.secret_connection_bundle_bytes,
            client_id=self.credentials.client_id,
            client_secret=self.credentials.client_secret,
        )
        self._lazy = lazy_init
        if not lazy_init:
            self._astra_client.connect()

    def _apply_filter(
        self,
        path: DataPath,
        filter_expression: Expression,
        columns: list[str],
        options: dict[QueryEnabledStoreOptions, any] | None = None,
        limit: Optional[int] = 10000,
    ) -> Union[MetaFrame, Iterator[MetaFrame]]:
        assert isinstance(path, AstraPath)
        astra_path: AstraPath = path
        if self._lazy:
            with self._astra_client as astra_client:
                return astra_client.filter_entities(
                    model_class=astra_path.model_class(),
                    key_column_filter_values=filter_expression,
                    keyspace=astra_path.keyspace,
                    table_name=astra_path.table,
                    select_columns=columns,
                    num_threads=-1,  # auto-infer, see method documentation
                    options=options,
                    limit=limit,
                )

        return self._astra_client.filter_entities(
            model_class=astra_path.model_class(),
            key_column_filter_values=filter_expression,
            keyspace=astra_path.keyspace,
            table_name=astra_path.table,
            select_columns=columns,
            num_threads=-1,  # auto-infer, see method documentation
            options=options,
            limit=limit,
        )

    def _apply_query(self, query: str) -> Union[MetaFrame, Iterator[MetaFrame]]:
        if self._lazy:
            with self._astra_client as astra_client:
                return astra_client.get_entities_raw(query)
        return self._astra_client.get_entities_raw(query)

    @classmethod
    def _from_connection_string(
        cls, connection_string: str, lazy_init: bool = False
    ) -> "QueryEnabledStore[AstraCredential, AstraSettings]":
        _, credentials, settings = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        return cls(
            credentials=AstraCredential.from_json(credentials),
            settings=AstraSettings.from_json(settings),
            lazy_init=lazy_init,
        )
