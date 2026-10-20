"""
QES implementations for Cassandra.
"""

import os
import re
from collections.abc import Iterator
from dataclasses import dataclass
from typing import final

from dataclasses_json import DataClassJsonMixin

from adapta.storage.distributed_object_store.v3.aws_keyspaces.keyspace_client import AwsKeyspaceClient
from adapta.storage.distributed_object_store.v3.vanilla_cassandra import VanillaCassandraClient
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
class CassandraCredential(DataClassJsonMixin):
    """
    Cassandra credential placeholder for QES. Authentication is performed via SigV4 using the AWS profile
    defined in settings, so no secrets are held here.
    """

    username: str | None = None
    password: str | None = None

    def __post_init__(self):
        self.username = self.username or os.getenv("ADAPTA__CASS_USERNAME")
        self.password = self.password or os.getenv("ADAPTA__CASS_PASSWORD")


@dataclass
class CassandraSettings(DataClassJsonMixin):
    """
    Cassandra connection settings for QES.

    :attr client_name: Name of the client reported to the cluster.
    :attr keyspace: Keyspace to connect to.
    :attr region: AWS region hosting the keyspace.
    :attr profile_name: AWS profile name used for SigV4 authentication.
    """

    client_name: str | None = None
    keyspace: str | None = None
    aws_region: str | None = None
    aws_profile_name: str | None = None
    contact_points: list[str] | None = None

    def __post_init__(self):
        self.client_name = self.client_name or os.getenv("ADAPTA__CASS_CLIENT_NAME")
        self.keyspace = self.keyspace or os.getenv("ADAPTA__CASS_KEYSPACE")
        self.aws_region = self.aws_region or os.getenv("ADAPTA__CASS_AWS_REGION")
        self.aws_profile_name = self.aws_profile_name or os.getenv("ADAPTA__CASS_AWS_PROFILE_NAME")
        self.contact_points = self.contact_points or os.getenv("ADAPTA__CONTACT_POINTS") or ["127.0.0.1"]

        if not all([self.keyspace, self.client_name]):
            raise RuntimeError(
                "Authentication information provided is insufficient. Please verify you are supplying keyspace and "
                "client name via connection string or via environment variables."
            )


@final
class CassandraQueryEnabledStore(QueryEnabledStore[CassandraCredential, CassandraSettings]):
    """
    QES Client for Cassandra.
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
            self._cassandra_client.disconnect()

    def __init__(self, credentials: CassandraCredential, settings: CassandraSettings, lazy_init: bool):
        super().__init__(credentials, settings)
        self._cassandra_client = self._resolve_client()
        self._lazy = lazy_init
        if not lazy_init:
            self._cassandra_client.connect()

    def _resolve_client(self):
        """
        Resolves the Cassandra client.
        """
        if self.settings.aws_region:
            return AwsKeyspaceClient(
                keyspace=self.settings.keyspace,
                client_name=self.settings.client_name,
                region=self.settings.aws_region,
                profile_name=self.settings.aws_profile_name,
                logger=self._logger,
                metrics_provider=self._metrics_provider,
            )

        return VanillaCassandraClient(
            keyspace=self.settings.keyspace,
            client_name=self.settings.client_name,
            username=self.credentials.username,
            password=self.credentials.password,
            contact_points=self.settings.contact_points,
            logger=self._logger,
            metrics_provider=self._metrics_provider,
        )

    def _apply_filter(
        self,
        path: DataPath,
        filter_expression: Expression,
        columns: list[str],
        options: dict[QueryEnabledStoreOptions, any] | None = None,
        limit: int | None = 10000,
        shard_count: int | None = None,
    ) -> MetaFrame | Iterator[MetaFrame]:
        assert isinstance(path, CassandraPath)
        cassandra_path: CassandraPath = path
        concat_options = options.get(QueryEnabledStoreOptions.CONCAT_OPTIONS, None) if options else None
        if self._lazy:
            with self._cassandra_client as cassandra_client:
                return cassandra_client.filter_entities(
                    model_class=cassandra_path.model_class(),
                    key_column_filter_values=filter_expression,
                    keyspace=cassandra_path.keyspace,
                    table_name=cassandra_path.table,
                    select_columns=columns,
                    num_threads=-1,  # auto-infer, see method documentation
                    concat_options=concat_options,
                    limit=limit,
                    shard_count=shard_count,
                )

        return self._cassandra_client.filter_entities(
            model_class=cassandra_path.model_class(),
            key_column_filter_values=filter_expression,
            keyspace=cassandra_path.keyspace,
            table_name=cassandra_path.table,
            select_columns=columns,
            num_threads=-1,  # auto-infer, see method documentation
            concat_options=concat_options,
            limit=limit,
            shard_count=shard_count,
        )

    def _apply_query(self, query: str) -> MetaFrame | Iterator[MetaFrame]:
        if self._lazy:
            with self._cassandra_client as cassandra_client:
                return cassandra_client.get_entities_raw(query)
        return self._cassandra_client.get_entities_raw(query)

    @classmethod
    def _from_connection_string(
        cls, connection_string: str, lazy_init: bool = False
    ) -> "QueryEnabledStore[CassandraCredential, CassandraSettings]":
        _, credentials, settings = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        return cls(
            credentials=CassandraCredential.from_json(credentials),
            settings=CassandraSettings.from_json(settings),
            lazy_init=lazy_init,
        )
