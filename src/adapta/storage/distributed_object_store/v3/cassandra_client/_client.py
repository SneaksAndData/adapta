import math
import os
import platform
import re
import ssl

from cassandra.cqlengine.connection import set_session

from adapta.storage.distributed_object_store.v3.cassandra_client._model_mappers import get_mapper

try:
    from _socket import IPPROTO_TCP, TCP_NODELAY, TCP_USER_TIMEOUT
except ImportError:
    # Fix for MacOS - MacOS does not have TCP_USER_TIMEOUT as in linux _socket module - https://man7.org/linux/man-pages/man7/tcp.7.html
    # So we removed TCP_USER_TIMEOUT from _socket import
    from socket import IPPROTO_TCP, TCP_NODELAY
from abc import ABC, abstractmethod
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from typing import Any, Self, TypeVar

import pandas
import polars
from backoff import expo, on_exception
from cassandra import ConsistencyLevel, WriteTimeout
from cassandra.auth import AuthProvider
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT,
    Cluster,
    ExecutionProfile,
    ExponentialReconnectionPolicy,
    RetryPolicy,
    Session,
)
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.named import NamedTable
from cassandra.cqlengine.query import BatchQuery, BatchType
from cassandra.metadata import TableMetadata, get_schema_parser
from cassandra.protocol import IsBootstrappingErrorMessage, OverloadedErrorMessage
from cassandra.query import dict_factory
from polars.exceptions import ComputeError

import adapta
from adapta.storage.distributed_object_store.v3.cassandra_client._client_configuration import (
    CassandraClientConfiguration,
)
from adapta.storage.models.enum import QueryEnabledStoreOptions
from adapta.storage.models.expression_dsl.astra_filter_expression import CassandraFilterExpression
from adapta.storage.models.expression_dsl.filter_expression import Expression, compile_expression
from adapta.utils import chunk_list, rate_limit
from adapta.utils.metaframe import MetaFrame, concat

TCassandraModel = TypeVar("TCassandraModel")


class CassandraClient(ABC):
    """Generic Cassandra client"""

    def __init__(
        self, client_name: str, keyspace: str | None, client_config: CassandraClientConfiguration | None
    ) -> None:
        self._client_name = client_name
        self._keyspace = keyspace
        self._session: Session | None = None
        self._cluster: Cluster | None = None
        self._snake_pattern = re.compile(r"(?<!^)(?=[A-Z])")
        self._filter_pattern = re.compile(r"(__\w+)")
        self._client_config = client_config or CassandraClientConfiguration.default()

    def connect(self) -> None:
        """Sync connect to this Cassandra cluster"""
        self._cluster = self._get_cluster()
        if self._cluster:
            self._session = self._cluster.connect(self._keyspace)
            set_session(self._session)

    @abstractmethod
    def _get_port(self) -> int | None:
        """Port number for this Cassandra client"""

    @abstractmethod
    def _get_contact_points(self) -> list[str] | None:
        """Coordinator addresses for this client"""

    @abstractmethod
    def _get_ssl_context(self) -> ssl.SSLContext | None:
        """SSL context for this client"""

    @abstractmethod
    def post_connect(self) -> None:
        """Cleanup or any extra config after connection, if needed"""

    @abstractmethod
    def _get_auth_provider(self) -> AuthProvider:
        """Auth provider for this client"""

    @abstractmethod
    def _cloud_config(self) -> dict | None:
        """Additional provider-specific configuration for this client"""

    def _get_cluster(self) -> Cluster:
        profile = ExecutionProfile(
            retry_policy=RetryPolicy(),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            request_timeout=self._client_config.socket_read_timeout_ms / 1e3,
            row_factory=dict_factory,
        )

        return Cluster(
            contact_points=self._get_contact_points(),
            port=self._get_port(),
            connection_class=self._client_config.connection_class,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            cloud=self._cloud_config(),
            auth_provider=self._get_auth_provider(),
            reconnection_policy=ExponentialReconnectionPolicy(
                self._client_config.reconnect_base_delay_ms, self._client_config.reconnect_max_delay_ms
            ),
            compression=True,
            ssl_context=self._get_ssl_context(),
            application_name=self._client_name,
            application_version=adapta.__version__,
            protocol_version=self._client_config.protocol_version,
            sockopts=[
                (IPPROTO_TCP, TCP_NODELAY, 1),
                (IPPROTO_TCP, TCP_USER_TIMEOUT, self._client_config.socket_read_timeout_ms),
            ]
            if platform.system().lower() != "darwin"
            else [(IPPROTO_TCP, TCP_NODELAY, 1)],
        )

    async def connect_async(self) -> Self:
        """Async connect to this Cassandra cluster"""
        self.connect()
        return self

    def disconnect(self) -> None:
        """
        Disconnect from the database and destroy the session.
        """
        if self._session and self._cluster:
            self._session.shutdown()
            self._cluster.shutdown()
            self._session = None
            self._cluster = None

    def __enter__(self) -> Self:
        """
        Creates an Astra client for this context.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    async def __aenter__(self) -> Self:
        await self.connect_async()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def get_table_metadata(self, table_name: str) -> TableMetadata:
        """
        Returns Cassandra table metadata.

        NB. Use the Force, read the Source: https://github.com/datastax/python-driver/blob/master/tests/integration/standard/test_metadata.py#L233-L238

        :param: table_name: Name of the table to read metadata for.
        """
        return get_schema_parser(self._session.cluster.control_connection._connection, "4-a", None, 0.1).get_table(
            keyspaces=None, keyspace=self._keyspace, table=table_name
        )

    def get_entity(self, table_name: str) -> dict:
        """
        Reads a single row from a table as dictionary
        https://docs.datastax.com/en/developer/python-driver/3.28/cqlengine/queryset/

        :param: table_name: Name of the table to read a row from.
        """

        named_table = NamedTable(self._keyspace, table_name)
        return named_table.objects[0]

    def get_entities_from_query(self, query: str, mapper: Callable[[dict], TCassandraModel]) -> MetaFrame:
        """
        Maps query result to a MetaFrame using custom mapper

        :param: query: A CQL query to execute.
        :param: mapper: A mapping function from a Dictionary to the desired model type.
        """
        if not self._session:
            raise RuntimeError("Cassandra session not initialized")
        return MetaFrame(
            [mapper(entity) for entity in self._session.execute(query)],
            convert_to_polars=polars.DataFrame,
            convert_to_pandas=pandas.DataFrame,
        )

    def filter_entities(
        self,
        model_class: type[TCassandraModel],
        key_column_filter_values: Expression | list[dict[str, Any]],
        keyspace: str | None = None,
        table_name: str | None = None,
        select_columns: list[str] | None = None,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        custom_indexes: list[str] | None = None,
        deduplicate=False,
        num_threads: int | None = None,
        options: dict[QueryEnabledStoreOptions, Any] | None = None,
        limit: int | None = None,
    ) -> MetaFrame:
        """
        Run a filter query on the entity of type TModel backed by table `table_name`.

        Example usage:

         @dataclass
         class Test:
             col_a: int
             col_b: str

         with AstraClient(...) as ac:
             data = ac.filter_entities("test_table", Test, ['col_a'], [{'col_a': 123},{'col_a': 345}])

        :param: model_class: A dataclass type that should be mapped to Astra Model.
        :param: key_column_filter_values: Primary key filters in a form of list of dictionaries of my_key: my_value. Multiple entries will result in multiple queries being run and concatenated
        :param: keyspace: Optional keyspace name, if not provided in the client constructor
        :param: table_name: Optional Astra table name, if it cannot be inferred from class name by converting it to snake_case.
        :param: select_columns: An optional list of columns to return with the query.
        :param: primary_keys: An optional list of columns that constitute a primary key, if it cannot be inferred from the data model.
        :param: partition_keys: An optional list of columns that constitute a partition key, if it cannot be inferred from the data model.
        :param: custom_indexes: An optional list of custom indexes, if it cannot be inferred, if it cannot be inferred from the data model.
        :param: deduplicate: Optionally deduplicate query result, for example when only the partition key part of a primary key is used to fetch results.
        :param: num_threads: Optionally run filtering using multiple threads. Setting this to -1 will cause this method to automatically evaluate number of threads based on filter expression size.
        :param: limit: Optionally limit the number of results returned. NOTE the limit works per call to Astra and not on the final result.
        """
        if options is None:
            options = {}

        @on_exception(
            wait_gen=expo,
            exception=(
                OverloadedErrorMessage,
                IsBootstrappingErrorMessage,
            ),
            max_tries=self._client_config.transient_error_max_retries,
            max_time=self._client_config.transient_error_max_wait_s,
            raise_on_giveup=True,
        )
        def apply(model: type[Model], key_column_filter: dict[str, Any], columns_to_select: list[str] | None):
            model = model.filter(**key_column_filter).limit(limit)
            if columns_to_select:
                return model.only(select_columns)

            return model

        def normalize_column_name(column_name: str) -> str:
            filter_suffix = re.findall(self._filter_pattern, column_name)
            if len(filter_suffix) == 0:
                return column_name

            return column_name.replace(filter_suffix[0], "")

        def convert_to_polars(x: list[dict]) -> polars.DataFrame:
            try:
                return polars.DataFrame(x, schema=select_columns)
            except ComputeError:
                # Catches errors related to incorrect schema inference and tries again with unlimited schema inference length
                return polars.DataFrame(x, schema=select_columns, infer_schema_length=None)

        def to_frame(
            model: type[Model], key_column_filter: dict[str, Any], columns_to_select: list[str] | None
        ) -> MetaFrame:
            return MetaFrame(
                [dict(v.items()) for v in list(apply(model, key_column_filter, columns_to_select))],
                convert_to_polars=convert_to_polars,
                convert_to_pandas=lambda x: pandas.DataFrame(x, columns=select_columns),
            )

        assert self._session is not None, (
            "Please instantiate an AstraClient using with AstraClient(...) before calling this method"
        )

        cassandra_model_mapper = get_mapper(
            data_model=model_class,
            keyspace=keyspace,
            table_name=table_name,
            primary_keys=primary_keys,
            partition_keys=partition_keys,
            custom_indexes=custom_indexes,
        )

        select_columns = (
            list(map(normalize_column_name, select_columns)) if select_columns else cassandra_model_mapper.column_names
        )

        cassandra_model = cassandra_model_mapper.map()

        compiled_filter_values = (
            compile_expression(key_column_filter_values, CassandraFilterExpression)
            if isinstance(key_column_filter_values, Expression)
            else key_column_filter_values
        )

        if num_threads:
            max_threads = (
                max([int(math.sqrt(len(compiled_filter_values) + 1) / 2), num_threads, os.cpu_count()])
                if num_threads == -1
                else num_threads
            )
            with ThreadPoolExecutor(max_workers=max_threads) as tpe:
                result = concat(
                    tpe.map(
                        lambda args: to_frame(*args),
                        [
                            (cassandra_model, key_column_filter, select_columns)
                            for key_column_filter in compiled_filter_values
                        ],
                        chunksize=max(int(len(compiled_filter_values) / num_threads), 1),
                    ),
                    options=options.get(QueryEnabledStoreOptions.CONCAT_OPTIONS, None),
                )
        else:
            result = concat(
                [
                    MetaFrame(
                        [dict(v.items()) for v in list(apply(cassandra_model, key_column_filter, select_columns))],
                        convert_to_polars=convert_to_polars
                        if not deduplicate
                        else (lambda x: convert_to_polars(x).unique()),
                        convert_to_pandas=(lambda x: pandas.DataFrame(x, columns=select_columns))
                        if not deduplicate
                        else (lambda x: pandas.DataFrame(x, columns=select_columns).drop_duplicates()),
                    )
                    for key_column_filter in compiled_filter_values
                ],
                options=options.get(QueryEnabledStoreOptions.CONCAT_OPTIONS, None),
            )

        return result

    def get_entities_raw(self, query: str) -> MetaFrame:
        """
         Maps query result to a MetaFrame

        :param: query: A CQL query to run.
        """
        return MetaFrame(
            self._session.execute(query), convert_to_polars=polars.DataFrame, convert_to_pandas=pandas.DataFrame
        )

    def set_table_option(self, table_name: str, option_name: str, option_value: str) -> None:
        """
        Sets a table property: https://docs.datastax.com/en/cql-oss/3.1/cql/cql_reference/tabProp.html

        :param: table_name: Table to set property for.
        :param: option_name: Table option to set value for.
        :param: option_value: Table option value to set.
        """
        self._session.execute(f"ALTER TABLE {self._keyspace}.{table_name} with {option_name}={option_value};")

    def delete_entity(
        self, entity: TCassandraModel, table_name: str | None = None, keyspace: str | None = None
    ) -> None:
        """
         Delete an entity from Astra table

        :param: entity: entity to delete
        :param: table_name: Table to delete entity from.
        :param: keyspace: Optional keyspace name, if not provided in the client constructor.
        """

        @on_exception(
            wait_gen=expo,
            exception=(
                OverloadedErrorMessage,
                IsBootstrappingErrorMessage,
            ),
            max_tries=self._transient_error_max_retries,
            max_time=self._transient_error_max_wait_s,
            raise_on_giveup=True,
        )
        def _delete_entity(model_class: type[Model], key_filter: dict):
            model_class.filter(**key_filter).delete()

        cassandra_mapper = get_mapper(
            data_model=type(entity),
            table_name=table_name,
            keyspace=keyspace,
        )

        _delete_entity(
            model_class=cassandra_mapper.map(),
            key_filter={key: getattr(entity, key) for key in cassandra_mapper.primary_keys},
        )

    def upsert_entity(
        self,
        entity: TCassandraModel,
        keyspace: str | None = None,
        table_name: str | None = None,
        client_rate_limit: str = "1000 per second",
        time_to_live: int | None = None,
    ) -> None:
        """
         Inserts a record into existing table.

        :param: entity: an object to insert
        :param: table_name: Table to insert entity into.
        :param: keyspace: Optional keyspace name, if not provided in the client constructor.
        :param: client_rate_limit: the limit string to parse (eg: "1 per hour"), default: "1000 per second"
        :param: time_to_live: Time to live in seconds for the inserted entity.
        """

        @on_exception(
            wait_gen=expo,
            exception=(OverloadedErrorMessage, IsBootstrappingErrorMessage, WriteTimeout),
            max_tries=self._transient_error_max_retries,
            max_time=self._transient_error_max_wait_s,
            raise_on_giveup=True,
        )
        @rate_limit(limit=client_rate_limit)
        def _save_entity(model_object: Model, ttl: int):
            model_object.ttl(ttl).save()

        cassandra_model = get_mapper(
            data_model=type(entity),
            table_name=table_name,
            keyspace=keyspace,
        ).map()

        _save_entity(cassandra_model(**asdict(entity)), ttl=time_to_live)

    def upsert_batch(
        self,
        entities: list[dict],
        entity_type: type[TCassandraModel],
        keyspace: str | None = None,
        table_name: str | None = None,
        batch_size=1000,
        client_rate_limit: str = "1000 per second",
        time_to_live: int | None = None,
    ) -> None:
        """
         Inserts a batch into existing table.

        :param: entities: entity batch to insert.
        :param: entity_type: type of entity in a batch.
        :param: keyspace: Optional keyspace name, if not provided in the client constructor.
        :param: table_name: Table to insert entity into.
        :param: batch_size: elements per batch to upsert.
        :param: client_rate_limit: the limit string to parse (eg: "1 per hour"), default: "1000 per second"
        :param: time_to_live: Time to live in seconds for the inserted entities.
        """

        @on_exception(
            wait_gen=expo,
            exception=(OverloadedErrorMessage, IsBootstrappingErrorMessage, WriteTimeout),
            max_tries=self._transient_error_max_retries,
            max_time=self._transient_error_max_wait_s,
            raise_on_giveup=True,
        )
        @rate_limit(limit=client_rate_limit)
        def _save_entities(model_class: type[Model], values: list[dict], ttl: int):
            with BatchQuery(batch_type=BatchType.UNLOGGED) as upsert_batch:
                for value in values:
                    model_class.batch(upsert_batch).ttl(ttl).create(**value)

        cassandra_model = get_mapper(
            data_model=entity_type,
            table_name=table_name,
            keyspace=keyspace,
        ).map()

        for chunk in chunk_list(entities, batch_size):
            _save_entities(
                model_class=cassandra_model,
                values=chunk,
                ttl=time_to_live,
            )
