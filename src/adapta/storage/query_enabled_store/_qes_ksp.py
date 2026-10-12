"""
QES implementations for AWS Keyspaces.
"""

import os
import re
from collections.abc import Iterator
from dataclasses import dataclass
from typing import final

from dataclasses_json import DataClassJsonMixin

from adapta._version import __version__
from adapta.storage.distributed_object_store.v3.aws_keyspaces.keyspace_client import AwsKeyspaceClient
from adapta.storage.models.base import DataPath
from adapta.storage.models.cassandra import CassandraPath
from adapta.storage.models.enum import QueryEnabledStoreOptions
from adapta.storage.models.expression_dsl.filter_expression import Expression
from adapta.storage.query_enabled_store._models import (
    CONNECTION_STRING_REGEX,
    QueryEnabledStore,
)
from adapta.utils.metaframe import MetaFrame


@dataclass
class KeyspacesCredential(DataClassJsonMixin):
    """
    AWS Keyspaces credential placeholder for QES. Authentication is performed via SigV4 using the AWS profile
    defined in settings, so no secrets are held here.
    """


@dataclass
class KeyspacesSettings(DataClassJsonMixin):
    """
    AWS Keyspaces connection settings for QES.

    :attr client_name: Name of the client reported to the cluster.
    :attr keyspace: Keyspace to connect to.
    :attr region: AWS region hosting the keyspace.
    :attr profile_name: AWS profile name used for SigV4 authentication.
    """

    client_name: str | None = None
    keyspace: str | None = None
    region: str | None = None
    profile_name: str | None = None

    def __post_init__(self):
        self.client_name = self.client_name or f"Adapta Client {__version__}"
        self.keyspace = self.keyspace or os.getenv("PROTEUS__KSP_KEYSPACE")
        self.region = self.region or os.getenv("PROTEUS__KSP_REGION")
        self.profile_name = self.profile_name or os.getenv("PROTEUS__KSP_PROFILE_NAME")

        if not self.region:
            raise RuntimeError(
                "AWS region is required. Please supply it either via connection string or via PROTEUS__KSP_REGION."
            )


@final
class KeyspacesQueryEnabledStore(QueryEnabledStore[KeyspacesCredential, KeyspacesSettings]):
    """
    QES Client for AWS Keyspaces (Cassandra).
    """

    def _write(
        self,
        path: DataPath,
        data: MetaFrame | Iterator[MetaFrame],
        block_size: int,
        overwrite: bool,
        merge_columns: list[str] | None = None,
        delete_filter: Expression | None = None,
    ) -> None:
        raise NotImplementedError("Writing not supported by this QES engine.")

    def close(self) -> None:
        if not self._lazy:
            self._keyspaces_client.disconnect()

    def __init__(self, credentials: KeyspacesCredential, settings: KeyspacesSettings, lazy_init: bool):
        super().__init__(credentials, settings)
        self._keyspaces_client = AwsKeyspaceClient(
            keyspace=self.settings.keyspace,
            client_name=self.settings.client_name,
            region=self.settings.region,
            profile_name=self.settings.profile_name,
        )
        self._lazy = lazy_init
        if not lazy_init:
            self._keyspaces_client.connect()

    def _apply_filter(
        self,
        path: DataPath,
        filter_expression: Expression,
        columns: list[str],
        options: dict[QueryEnabledStoreOptions, any] | None = None,
        limit: int | None = 10000,
    ) -> MetaFrame | Iterator[MetaFrame]:
        assert isinstance(path, CassandraPath)
        cassandra_path: CassandraPath = path
        if self._lazy:
            with self._keyspaces_client as keyspaces_client:
                return keyspaces_client.filter_entities(
                    model_class=cassandra_path.model_class(),
                    key_column_filter_values=filter_expression,
                    keyspace=cassandra_path.keyspace,
                    table_name=cassandra_path.table,
                    select_columns=columns,
                    num_threads=-1,  # auto-infer, see method documentation
                    options=options,
                    limit=limit,
                )

        return self._keyspaces_client.filter_entities(
            model_class=cassandra_path.model_class(),
            key_column_filter_values=filter_expression,
            keyspace=cassandra_path.keyspace,
            table_name=cassandra_path.table,
            select_columns=columns,
            num_threads=-1,  # auto-infer, see method documentation
            options=options,
            limit=limit,
        )

    def _apply_query(self, query: str) -> MetaFrame | Iterator[MetaFrame]:
        if self._lazy:
            with self._keyspaces_client as keyspaces_client:
                return keyspaces_client.get_entities_raw(query)
        return self._keyspaces_client.get_entities_raw(query)

    @classmethod
    def _from_connection_string(
        cls, connection_string: str, lazy_init: bool = False
    ) -> "QueryEnabledStore[KeyspacesCredential, KeyspacesSettings]":
        _, credentials, settings = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        return cls(
            credentials=KeyspacesCredential.from_json(credentials),
            settings=KeyspacesSettings.from_json(settings),
            lazy_init=lazy_init,
        )
