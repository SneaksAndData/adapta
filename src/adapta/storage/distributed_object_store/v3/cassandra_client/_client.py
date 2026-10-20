import atexit
import logging
import math
import os
import platform
import re
import ssl
import sys
import tempfile
import uuid

from cassandra.cqlengine.connection import set_session
from cassandra.cqlengine.management import sync_table
from cassandra.query import PreparedStatement

from adapta.logs.handlers.safe_stream_handler import SafeStreamHandler
from adapta.storage.distributed_object_store.v3.cassandra_client._model_mappers import (
    CassandraModelMapper,
    get_mapper,
)

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
from cassandra import ConsistencyLevel, WriteFailure, WriteTimeout
from cassandra.auth import AuthProvider
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT,
    Cluster,
    ExecutionProfile,
    ExponentialReconnectionPolicy,
    RetryPolicy,
    Session,
)
from cassandra.concurrent import execute_concurrent
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.named import NamedTable
from cassandra.cqlengine.query import BatchQuery, BatchType
from cassandra.metadata import TableMetadata, get_schema_parser
from cassandra.protocol import IsBootstrappingErrorMessage, OverloadedErrorMessage
from cassandra.query import dict_factory
from polars.exceptions import ComputeError

import adapta
from adapta.logs import LoggerInterface, VoidLogger
from adapta.metrics import MetricsProvider
from adapta.metrics.providers.void_provider import VoidMetricsProvider
from adapta.storage.distributed_object_store.v3.cassandra_client._client_configuration import (
    CassandraClientConfiguration,
)
from adapta.storage.models.expression_dsl.cassandra_filter_expression import CassandraFilterExpression
from adapta.storage.models.expression_dsl.filter_expression import Expression, compile_expression
from adapta.utils import chunk_list, rate_limit, run_time_metrics
from adapta.utils.concurrent_task_runner import ConcurrentTaskRunner, Executable
from adapta.utils.metaframe import MetaFrame, MetaFrameOptions, concat

TCassandraModel = TypeVar("TCassandraModel")


def _prepare_lazy_source(source: polars.LazyFrame | polars.DataFrame) -> polars.LazyFrame:
    """
    Prepares a Polars LazyFrame or DataFrame, casting any list[struct{"key": ..., "value": ...}] columns to native Map types.
    """
    lazy_source = source.lazy() if isinstance(source, polars.DataFrame) else source
    schema = lazy_source.collect_schema()
    map_transforms = []
    for col_name, dtype in schema.items():
        if isinstance(dtype, (polars.List, polars.Array)) and isinstance(dtype.inner, polars.Struct):
            fields = {f.name: f.dtype for f in dtype.inner.fields}
            if len(fields) == 2 and "key" in fields and "value" in fields:
                map_transforms.append(polars.col(col_name).cast(polars.Map(fields["key"], fields["value"])))

    if map_transforms:
        lazy_source = lazy_source.with_columns(*map_transforms)

    return lazy_source


def _shard_source(lazy_source: polars.LazyFrame, shard_count: int) -> list[polars.LazyFrame]:
    """
    Splits a Polars LazyFrame into shard_count buckets using the 'rowhash' column.
    """
    schema = lazy_source.collect_schema()
    if "rowhash" not in schema:
        raise ValueError("A 'rowhash' column is required for sharded uploads (shard_count > 1)")

    bucket_expr = polars.col("rowhash").str.replace("^0x", "").str.slice(0, 8).str.to_integer(base=16) % shard_count

    with tempfile.NamedTemporaryFile(suffix=".ipc", delete=False) as tmp_file:
        tmp_path = tmp_file.name

    lazy_source.sink_ipc(tmp_path)
    scan = polars.scan_ipc(tmp_path)
    atexit.register(lambda: os.path.exists(tmp_path) and os.remove(tmp_path))

    source_with_bucket = scan.with_columns(bucket_expr.alias("__bucket"))
    return [source_with_bucket.filter(polars.col("__bucket") == i).drop("__bucket") for i in range(shard_count)]


def _fix_map_type(entity: dict) -> dict:
    """Polars <2 remaps map<k, v> to list[{"key": ..., "value": ...}]"""
    fixed = {}
    for cell_key, cell_value in entity.items():
        if isinstance(cell_value, list) and len(cell_value) > 0 and "key" in cell_value[0] and "value" in cell_value[0]:
            fixed[cell_key] = {a["key"]: a["value"] for a in cell_value}
        else:
            fixed[cell_key] = cell_value
    return fixed


class CassandraClient(ABC):
    """Generic Cassandra client"""

    def __init__(
        self,
        client_name: str,
        keyspace: str | None,
        client_config: CassandraClientConfiguration | None = None,
        logger: LoggerInterface | None = None,
        metrics_provider: MetricsProvider | None = None,
    ) -> None:
        self._client_name = client_name
        self._keyspace = keyspace
        self._logger = logger or VoidLogger()
        self._metrics_provider = metrics_provider or VoidMetricsProvider()
        self._session: Session | None = None
        self._cluster: Cluster | None = None
        self._snake_pattern = re.compile(r"(?<!^)(?=[A-Z])")
        self._filter_pattern = re.compile(r"(__\w+)")
        self._client_config = client_config or CassandraClientConfiguration.default()
        if self._client_config.log_transient_errors:
            logging.getLogger("backoff").addHandler(SafeStreamHandler(sys.stdout))

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

        cloud = self._cloud_config()
        if cloud:
            return Cluster(
                port=self._get_port(),
                connection_class=self._client_config.connection_class,
                execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                cloud=self._cloud_config(),
                auth_provider=self._get_auth_provider(),
                reconnection_policy=ExponentialReconnectionPolicy(
                    self._client_config.reconnect_base_delay_ms, self._client_config.reconnect_max_delay_ms
                ),
                compression=True,
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

        return Cluster(
            contact_points=self._get_contact_points(),
            port=self._get_port(),
            connection_class=self._client_config.connection_class,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
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
        Creates an Cassandra client for this context.
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

    def create_table(self, entity: TCassandraModel, table_name: str, keyspace: str) -> None:
        """
        Creates a table with the given name, in a given keyspaces, in compliance with the specified model.
        """
        cassandra_mapper = get_mapper(
            data_model=entity if isinstance(entity, type) else type(entity),
            table_name=table_name,
            keyspace=keyspace,
        )

        sync_table(cassandra_mapper.map())

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
        concat_options: list[MetaFrameOptions] | None = None,
        limit: int | None = None,
        shard_count: int | None = None,
    ) -> MetaFrame:
        """
        Run a filter query on the entity of type TModel backed by table `table_name`.

        Example usage:

         @dataclass
         class Test:
             col_a: int
             col_b: str

         with CassandraClient(...) as ac:
             data = ac.filter_entities("test_table", Test, ['col_a'], [{'col_a': 123},{'col_a': 345}])

        :param: model_class: A dataclass type that should be mapped to Cassandra Model.
        :param: key_column_filter_values: Primary key filters in a form of list of dictionaries of my_key: my_value. Multiple entries will result in multiple queries being run and concatenated
        :param: keyspace: Optional keyspace name, if not provided in the client constructor
        :param: table_name: Optional Cassandra table name, if it cannot be inferred from class name by converting it to snake_case.
        :param: select_columns: An optional list of columns to return with the query.
        :param: primary_keys: An optional list of columns that constitute a primary key, if it cannot be inferred from the data model.
        :param: partition_keys: An optional list of columns that constitute a partition key, if it cannot be inferred from the data model.
        :param: custom_indexes: An optional list of custom indexes, if it cannot be inferred, if it cannot be inferred from the data model.
        :param: deduplicate: Optionally deduplicate query result, for example when only the partition key part of a primary key is used to fetch results.
        :param: num_threads: Optionally run filtering using multiple threads. Setting this to -1 will cause this method to automatically evaluate number of threads based on filter expression size.
        :param: concat_options: Optional list of MetaFrameOptions passed to concat when combining query results.
        :param: limit: Optionally limit the number of results returned. NOTE the limit works per call to Cassandra and not on the final result.
        :param: shard_count: Number of shards if the table is sharded into {table_name}_{i} tables. When specified (>1), all shard tables are queried concurrently and combined into a single MetaFrame.
        """
        if shard_count is not None and shard_count <= 0:
            raise ValueError("shard_count must be greater than 0")

        if shard_count and shard_count > 1:
            cassandra_model_mapper = get_mapper(
                data_model=model_class,
                keyspace=keyspace,
                table_name=table_name,
                primary_keys=primary_keys,
                partition_keys=partition_keys,
                custom_indexes=custom_indexes,
            )
            resolved_table_name = table_name or cassandra_model_mapper.table_name

            def _filter_shard(shard_index: int) -> MetaFrame:
                return self.filter_entities(
                    model_class=model_class,
                    key_column_filter_values=key_column_filter_values,
                    keyspace=keyspace,
                    table_name=f"{resolved_table_name}_{shard_index}",
                    select_columns=select_columns,
                    primary_keys=primary_keys,
                    partition_keys=partition_keys,
                    custom_indexes=custom_indexes,
                    deduplicate=deduplicate,
                    num_threads=num_threads,
                    concat_options=concat_options,
                    limit=limit,
                )

            ctr = ConcurrentTaskRunner[MetaFrame](
                [
                    Executable(
                        _filter_shard,
                        str(uuid.uuid4()),
                        args=[i],
                    )
                    for i in range(shard_count)
                ],
                num_threads=shard_count,
            )
            shard_frames = list(ctr.eager().values())
            res = concat(shard_frames, options=concat_options)
            if deduplicate:
                return MetaFrame(
                    data=res,
                    convert_to_polars=lambda r: r.to_polars().unique(),
                    convert_to_pandas=lambda r: r.to_pandas().drop_duplicates(),
                )
            return res

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
            if len(key_column_filter) == 0:
                model = model.all().limit(limit)
            else:
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
                for row in x:
                    for col in select_columns:
                        if isinstance(row[col], dict):
                            row[col] = [{"key": k, "value": v} for k, v in row[col].items()]
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
            "Please connect to Cassandra using connect() or with CassandraClient(...) before calling this method"
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
            else (key_column_filter_values or [])
        )

        # return entire table if no filter was supplied
        if len(compiled_filter_values) == 0:
            return MetaFrame(
                [dict(v.items()) for v in list(apply(cassandra_model, {}, select_columns))],
                convert_to_polars=convert_to_polars if not deduplicate else (lambda x: convert_to_polars(x).unique()),
                convert_to_pandas=(lambda x: pandas.DataFrame(x, columns=select_columns))
                if not deduplicate
                else (lambda x: pandas.DataFrame(x, columns=select_columns).drop_duplicates()),
            )

        if num_threads:
            max_threads = (
                max([int(math.sqrt(len(compiled_filter_values) + 1) / 2), num_threads, os.cpu_count()])
                if num_threads == -1
                else num_threads
            )
            with ThreadPoolExecutor(max_workers=max_threads) as tpe:
                return concat(
                    tpe.map(
                        lambda args: to_frame(*args),
                        [
                            (cassandra_model, key_column_filter, select_columns)
                            for key_column_filter in compiled_filter_values
                        ],
                        chunksize=max(int(len(compiled_filter_values) / num_threads), 1),
                    ),
                    options=concat_options,
                )
        else:
            return concat(
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
                options=concat_options,
            )

    def get_entities_raw(self, query: str, force_eager: bool = False) -> MetaFrame:
        """
         Maps query result to a MetaFrame

        :param: query: A CQL query to run.
        """
        if force_eager:
            return MetaFrame(
                self._session.execute(query).all(),
                convert_to_polars=polars.DataFrame,
                convert_to_pandas=pandas.DataFrame,
            )
        return MetaFrame(
            self._session.execute(query),
            convert_to_polars=lambda result_set: polars.DataFrame(dict(row) for row in result_set),
            convert_to_pandas=pandas.DataFrame,
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
         Delete an entity from Cassandra table

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
            max_tries=self._client_config.transient_error_max_retries,
            max_time=self._client_config.transient_error_max_wait_s,
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

    def delete_entity_by_key(
        self,
        model: type[TCassandraModel],
        primary_keys: dict[str, Any],
        table_name: str | None = None,
        keyspace: str | None = None,
    ) -> None:
        """
        Delete an entity from Cassandra table, using only primary and model information
        """

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
        def _delete_entity(model_class: type[Model], key_filter: dict):
            model_class.filter(**key_filter).delete()

        cassandra_mapper = get_mapper(
            data_model=model,
            table_name=table_name,
            keyspace=keyspace,
        )

        # discard all data but primary keys
        delete_filter = {pk: pk_value for pk, pk_value in primary_keys.items() if pk in cassandra_mapper.primary_keys}

        _delete_entity(
            model_class=cassandra_mapper.map(),
            key_filter=delete_filter,
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
            max_tries=self._client_config.transient_error_max_retries,
            max_time=self._client_config.transient_error_max_wait_s,
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
            exception=(OverloadedErrorMessage, IsBootstrappingErrorMessage, WriteTimeout, WriteFailure),
            max_tries=self._client_config.transient_error_max_retries,
            max_time=self._client_config.transient_error_max_wait_s,
            raise_on_giveup=True,
        )
        @rate_limit(limit=client_rate_limit)
        def _save_entities(model_class: type[Model], values: list[dict], ttl: int | None):
            with BatchQuery(batch_type=BatchType.Unlogged) as upsert_batch:
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

    def _prepare_insert_statement(self, table_identifier: str, columns: list[str]) -> PreparedStatement:
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["?" for _ in columns])
        query = f"INSERT INTO {table_identifier} ({columns_str}) VALUES ({placeholders});"
        return self._session.prepare(query)

    def _build_statements_and_params(
        self,
        rows: list[dict] | polars.LazyFrame,
        table_identifier: str,
        cassandra_mapper: CassandraModelMapper,
    ) -> list[tuple[PreparedStatement, tuple]]:
        if isinstance(rows, polars.LazyFrame):
            batch_rows = _prepare_lazy_source(rows)
            batch_columns = [col for col in cassandra_mapper.column_names if col in batch_rows.collect_schema()]
            if not batch_columns:
                return []

            prepared_rows = batch_rows.select(batch_columns).collect()
            if prepared_rows.is_empty():
                return []

            prepared_stmt = self._prepare_insert_statement(table_identifier, batch_columns)
            return [(prepared_stmt, row) for row in prepared_rows.iter_rows()]

        if not rows:
            return []

        prepared_stmt = self._prepare_insert_statement(table_identifier, cassandra_mapper.column_names)
        return [(prepared_stmt, tuple(row.get(col) for col in cassandra_mapper.column_names)) for row in rows]

    def upsert_concurrent(
        self,
        rows: list[dict] | polars.LazyFrame,
        table_name: str,
        entity_type: type[TCassandraModel],
        keyspace: str | None = None,
        concurrency: int | None = None,
    ) -> None:
        """
        Upserts rows concurrently using cassandra.concurrent.execute_concurrent.

        :param rows: List of dictionaries or a Polars LazyFrame representing rows to upsert.
        :param table_name: Table to insert entities into.
        :param entity_type: Entity type to map data model and metadata.
        :param keyspace: Optional keyspace name, if not provided in the client constructor.
        :param concurrency: Maximum number of concurrent statements. Defaults to cpu_count * 2.
        """
        assert self._session is not None, (
            "Please connect to Cassandra using connect() or with CassandraClient(...) before calling this method"
        )

        target_keyspace = keyspace or self._keyspace

        cassandra_mapper = get_mapper(
            data_model=entity_type,
            table_name=table_name,
            keyspace=target_keyspace,
        )
        target_table_name = table_name or cassandra_mapper.table_name

        table_identifier = f"{target_keyspace}.{target_table_name}" if target_keyspace else target_table_name

        statements_and_params = self._build_statements_and_params(
            rows=rows,
            table_identifier=table_identifier,
            cassandra_mapper=cassandra_mapper,
        )
        if not statements_and_params:
            return

        concurrency = concurrency or (os.cpu_count() or 1) * 2

        @on_exception(
            wait_gen=expo,
            exception=(OverloadedErrorMessage, IsBootstrappingErrorMessage, WriteTimeout, WriteFailure),
            max_tries=self._client_config.transient_error_max_retries,
            max_time=self._client_config.transient_error_max_wait_s,
            max_value=self._client_config.transient_error_max_wait_s_per_retry,
            raise_on_giveup=True,
        )
        def _execute_concurrent():
            execute_concurrent(
                self._session,
                statements_and_params,
                concurrency=concurrency,
            )

        _execute_concurrent()

    def upload_concurrent_native(
        self,
        source: polars.LazyFrame | polars.DataFrame,
        table_name: str,
        entity_type: type[TCassandraModel],
        keyspace: str | None = None,
        batch_size: int = 100_000,
        threads: int | None = None,
        shard_count: int | None = None,
    ) -> int:
        """
        Uploads a Polars LazyFrame or DataFrame in batches using upsert_concurrent.

        Note: Map type column adjustments are applied to lazy_source as native Polars Map types.

        :param source: Polars LazyFrame or DataFrame to upload.
        :param table_name: Table to insert entities into.
        :param entity_type: Entity type to map data model and metadata.
        :param keyspace: Optional keyspace name, if not provided in the client constructor.
        :param batch_size: Number of rows per batch to collect and upsert. Defaults to 100,000.
        :param threads: Maximum number of concurrent statements per batch.
        :param shard_count: Number of shards to split rows into for parallel upload. When specified (>1), rows are split into buckets by 'rowhash' and uploaded concurrently using ConcurrentTaskRunner.
        :return: Total number of rows upserted.
        """
        if shard_count is not None and shard_count <= 0:
            raise ValueError("shard_count must be greater than 0")

        lazy_source = _prepare_lazy_source(source)

        @run_time_metrics(metric_name="adapta.cassandra.iceberg_batch_upsert_duration")
        def _upsert_with_metric(entities: list[dict] | polars.LazyFrame, **kwargs) -> int:
            self.upsert_concurrent(
                rows=entities,
                table_name=kwargs["table_name"],
                entity_type=kwargs["entity_type"],
                keyspace=kwargs["keyspace"],
                concurrency=kwargs.get("concurrency"),
            )
            row_count = kwargs.get("row_count") or (len(entities) if isinstance(entities, list) else 0)
            self._metrics_provider.gauge("adapta.cassandra.upserted_rows", row_count)
            self._logger.info("Upserted {rows} rows", rows=row_count)
            return row_count

        def _upload_batches(source_batches: polars.LazyFrame, target_table: str) -> int:
            total = 0
            for batch in source_batches.collect_batches(chunk_size=batch_size, maintain_order=False):
                _upsert_with_metric(
                    batch.lazy(),
                    table_name=target_table,
                    entity_type=entity_type,
                    keyspace=keyspace,
                    concurrency=threads,
                    row_count=batch.height,
                    metrics_provider=self._metrics_provider,
                    logger=self._logger,
                )
                total += batch.height
            return total

        if shard_count and shard_count > 1:
            shards = _shard_source(lazy_source, shard_count)
            ctr = ConcurrentTaskRunner[int](
                [
                    Executable(
                        _upload_batches,
                        str(uuid.uuid4()),
                        args=[shard, f"{table_name}_{i}"],
                    )
                    for i, shard in enumerate(shards)
                ],
                num_threads=shard_count,
            )
            return sum([rows_synced for _, rows_synced in ctr.eager().items()])

        return _upload_batches(lazy_source, table_name)

    def upload_concurrent_batch(
        self,
        source: polars.LazyFrame | polars.DataFrame,
        table_name: str,
        entity_type: type[TCassandraModel],
        keyspace: str | None = None,
        batch_size: int = 1_000,
        threads: int = 4,
        shard_count: int | None = None,
    ) -> int:
        """
        Uploads a Polars LazyFrame or DataFrame concurrently using Cassandra batch upserts and ConcurrentTaskRunner.

        Note: Map type column adjustments are applied to lazy_source as native Polars Map types.

        :param source: Polars LazyFrame or DataFrame to upload.
        :param table_name: Table to insert entities into.
        :param entity_type: Entity type to map data model and metadata.
        :param keyspace: Optional keyspace name, if not provided in the client constructor.
        :param batch_size: Number of rows per batch to collect and upsert. Defaults to 1,000.
        :param threads: Number of parallel threads to use. Defaults to 4.
        :param shard_count: Number of shards to split rows into for parallel upload. When specified (>1), rows are split into buckets by 'rowhash' and uploaded concurrently using ConcurrentTaskRunner.
        """
        if shard_count is not None and shard_count <= 0:
            raise ValueError("shard_count must be greater than 0")

        lazy_source = _prepare_lazy_source(source)

        @run_time_metrics(metric_name="adapta.cassandra.batch_upsert_duration")
        def _upsert_with_metric(entities: list[dict], **kwargs) -> int:
            self.upsert_batch(
                entities=entities,
                entity_type=kwargs["entity_type"],
                keyspace=kwargs["keyspace"],
                table_name=kwargs["table_name"],
                batch_size=kwargs["batch_size"],
            )
            self._metrics_provider.gauge("adapta.cassandra.upserted_rows", len(entities))
            self._logger.info("Upserted {rows} rows", rows=len(entities))
            return len(entities)

        if shard_count and shard_count > 1:

            def _upload_shard(shard_source: polars.LazyFrame, target_table: str) -> int:
                shard_rows = 0
                for source_batch in shard_source.collect_batches(chunk_size=batch_size, maintain_order=False):
                    _upsert_with_metric(
                        source_batch.to_dicts(),
                        entity_type=entity_type,
                        keyspace=keyspace or self._keyspace,
                        table_name=target_table,
                        batch_size=source_batch.height,
                        metrics_provider=self._metrics_provider,
                        logger=self._logger,
                    )
                    shard_rows += source_batch.height
                return shard_rows

            shards = _shard_source(lazy_source, shard_count)
            ctr = ConcurrentTaskRunner[int](
                [
                    Executable(
                        _upload_shard,
                        str(uuid.uuid4()),
                        args=[shard, f"{table_name}_{i}"],
                    )
                    for i, shard in enumerate(shards)
                ],
                num_threads=shard_count,
            )
            return sum([rows_synced for _, rows_synced in ctr.eager().items()])

        ctr = ConcurrentTaskRunner[int](
            [
                Executable(
                    _upsert_with_metric,
                    str(uuid.uuid4()),
                    kwargs={
                        "entities": source_batch.to_dicts(),
                        "entity_type": entity_type,
                        "keyspace": keyspace or self._keyspace,
                        "table_name": table_name,
                        "batch_size": source_batch.height,
                        "metrics_provider": self._metrics_provider,
                        "logger": self._logger,
                    },
                )
                for source_batch in lazy_source.collect_batches(chunk_size=batch_size, maintain_order=False)
            ],
            num_threads=threads,
        )
        return sum([rows_synced for _, rows_synced in ctr.eager().items()])

    def delete_batch_concurrent(
        self,
        source: polars.LazyFrame | polars.DataFrame,
        table_name: str,
        entity_type: type[TCassandraModel],
        keyspace: str | None = None,
        batch_size: int = 1_000,
        threads: int = 4,
        shard_count: int | None = None,
    ) -> int:
        """
        Deletes entities in batches from a Polars LazyFrame or DataFrame concurrently using ConcurrentTaskRunner.

        Note: Map type column adjustments are applied to lazy_source as native Polars Map types.

        :param source: Polars LazyFrame or DataFrame containing entities to delete.
        :param table_name: Table to delete entities from.
        :param entity_type: Entity type to map data model and metadata.
        :param keyspace: Optional keyspace name, if not provided in the client constructor.
        :param batch_size: Number of rows per batch to collect and delete. Defaults to 1,000.
        :param threads: Number of parallel threads to use. Defaults to 4.
        :param shard_count: Number of shards if the table is sharded. When specified (>1), entities are deleted from all shard tables concurrently using ConcurrentTaskRunner.
        """
        if shard_count is not None and shard_count <= 0:
            raise ValueError("shard_count must be greater than 0")

        lazy_source = _prepare_lazy_source(source)

        target_keyspace = keyspace or self._keyspace
        cassandra_mapper = get_mapper(
            data_model=entity_type,
            table_name=table_name,
            keyspace=target_keyspace,
        )
        primary_keys = cassandra_mapper.primary_keys

        @run_time_metrics(metric_name="adapta.cassandra.batch_upsert_duration")
        def _delete_with_metric(entities: list[dict], **kwargs) -> int:
            for entity in entities:
                self.delete_entity_by_key(
                    model=kwargs["entity_type"],
                    primary_keys={key: entity[key] for key in kwargs["primary_keys"]},
                    keyspace=kwargs["keyspace"],
                    table_name=kwargs["table_name"],
                )
            self._metrics_provider.gauge("adapta.cassandra.deleted_rows", len(entities))
            self._logger.info("Deleted {rows} rows", rows=len(entities))
            return len(entities)

        if shard_count and shard_count > 1:
            with tempfile.NamedTemporaryFile(suffix=".ipc", delete=False) as tmp_file:
                tmp_path = tmp_file.name

            lazy_source.sink_ipc(tmp_path)
            scan = polars.scan_ipc(tmp_path)
            atexit.register(lambda: os.path.exists(tmp_path) and os.remove(tmp_path))
            lazy_source = scan

            def _delete_shard(shard_source: polars.LazyFrame, target_table: str) -> int:
                shard_rows = 0
                for source_batch in shard_source.collect_batches(chunk_size=batch_size, maintain_order=False):
                    _delete_with_metric(
                        source_batch.to_dicts(),
                        entity_type=entity_type,
                        keyspace=target_keyspace,
                        table_name=target_table,
                        primary_keys=primary_keys,
                        metrics_provider=self._metrics_provider,
                        logger=self._logger,
                    )
                    shard_rows += source_batch.height
                return shard_rows

            ctr = ConcurrentTaskRunner[int](
                [
                    Executable(
                        _delete_shard,
                        str(uuid.uuid4()),
                        args=[lazy_source, f"{table_name}_{i}"],
                    )
                    for i in range(shard_count)
                ],
                num_threads=shard_count,
            )
            return sum([rows_deleted for _, rows_deleted in ctr.eager().items()])

        ctr = ConcurrentTaskRunner[int](
            [
                Executable(
                    _delete_with_metric,
                    str(uuid.uuid4()),
                    kwargs={
                        "entities": source_batch.to_dicts(),
                        "entity_type": entity_type,
                        "keyspace": target_keyspace,
                        "table_name": table_name,
                        "primary_keys": primary_keys,
                        "metrics_provider": self._metrics_provider,
                        "logger": self._logger,
                    },
                )
                for source_batch in lazy_source.collect_batches(chunk_size=batch_size, maintain_order=False)
            ],
            num_threads=threads,
        )
        return sum([rows_deleted for _, rows_deleted in ctr.eager().items()])
