"""
 DataStax Astra client driver.
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

import base64
import logging
import math
import os
import platform
import re
import tempfile
import typing
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from typing import Optional, Dict, TypeVar, Callable, Type, List, Any, Union

try:
    from _socket import IPPROTO_TCP, TCP_NODELAY, TCP_USER_TIMEOUT
except ImportError:
    # Fix for MacOS - MacOS does not have TCP_USER_TIMEOUT as in linux _socket module - https://man7.org/linux/man-pages/man7/tcp.7.html
    # So we removed TCP_USER_TIMEOUT from _socket import
    from socket import IPPROTO_TCP, TCP_NODELAY

from backoff import on_exception, expo
import pandas
import polars
from cassandra import ConsistencyLevel, WriteTimeout
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (  # pylint: disable=E0611
    Cluster,
    Session,
    RetryPolicy,
    ExecutionProfile,
    EXEC_PROFILE_DEFAULT,
)
from cassandra.cqlengine.connection import set_session
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.named import NamedTable
from cassandra.cqlengine.query import BatchQuery
from cassandra.metadata import TableMetadata, get_schema_parser  # pylint: disable=E0611
from cassandra.policies import ExponentialReconnectionPolicy
from cassandra.protocol import OverloadedErrorMessage, IsBootstrappingErrorMessage  # pylint: disable=E0611
from cassandra.query import dict_factory, BatchType  # pylint: disable=E0611

from adapta import __version__
from adapta.storage.distributed_object_store.v3.datastax_astra._models import SimilarityFunction, VectorSearchQuery
from adapta.storage.models.filter_expression import Expression, AstraFilterExpression, compile_expression
from adapta.utils import chunk_list, rate_limit
from adapta.utils.metaframe import MetaFrame, concat
from adapta.storage.distributed_object_store.v3.datastax_astra._model_mappers import get_mapper

TModel = TypeVar("TModel")  # pylint: disable=C0103


@typing.final
class AstraClient:
    """
    DataStax Astra (https://astra.datastax.com) credentials provider.

    EXPERIMENTAL API NOTICE: THIS API IS NOT FINALIZED AND IS A SUBJECT TO CHANGE. USE WITH CAUTION UNTIL IT GRADUATES TO STABLE.

     :param: client_name: Arbitrary string that represents the connecting client in the database.
     :param: keyspace: Keyspace to scope queries to.
     :param: secure_connect_bundle_bytes: Base64-encoded contents (string) of a secure connect bundle.
     :param: client_id: Astra token client_id
     :param: client_secret: Astra token client secret
     :param: reconnect_base_delay_ms: Reconnect delay in ms, in case of a connection or node failure (min value for exp. backoff).
     :param: reconnect_max_delay_ms: Reconnect delay in ms, in case of a connection or node failure (max value for exp backoff).
     :param: socket_connection_timeout: Connect timeout for the TCP connection.
     :param: socket_read_timeout: Read timeout for TCP operations (query timeout).
     :param: transient_error_max_retries: Maximum number of exp backoff retries for transient errors like rate limit.
     :param: transient_error_max_wait_s: Maximum cumulative wait time for exp backoff attempts for transient errors.
     :param: log_transient_errors: Whether to log errors that can be resolved via exp backoff retries.
     :param: metadata_fetch_timeout_s: Timeout in seconds for the driver’s HTTP call to get cluster metadata from Astra DB. Defaults to 30s up fromf factory default of 5 seconds.
    """

    def __init__(
        self,
        client_name: str,
        keyspace: Optional[str] = None,
        secure_connect_bundle_bytes: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        reconnect_base_delay_ms=1000,
        reconnect_max_delay_ms=5000,
        socket_connection_timeout_ms=5000,
        socket_read_timeout_ms=180000,
        transient_error_max_retries=10,
        transient_error_max_wait_s=300,
        log_transient_errors=True,
        metadata_fetch_timeout_s=30,
    ):
        self._secure_connect_bundle_bytes = secure_connect_bundle_bytes or os.getenv("PROTEUS__ASTRA_BUNDLE_BYTES")
        self._client_id = client_id or os.getenv("PROTEUS__ASTRA_CLIENT_ID")
        self._client_secret = client_secret or os.getenv("PROTEUS__ASTRA_CLIENT_SECRET")
        self._keyspace = keyspace
        self._tmp_bundle_path = os.path.join(tempfile.gettempdir(), ".astra")
        self._client_name = client_name
        self._session: Optional[Session] = None
        self._reconnect_base_delay_ms = reconnect_base_delay_ms
        self._reconnect_max_delay_ms = reconnect_max_delay_ms
        self._socket_connection_timeout = socket_connection_timeout_ms
        self._socket_read_timeout = socket_read_timeout_ms
        self._query_timeout = socket_read_timeout_ms
        self._snake_pattern = re.compile(r"(?<!^)(?=[A-Z])")
        self._filter_pattern = re.compile(r"(__\w+)")
        self._transient_error_max_retries = transient_error_max_retries
        self._transient_error_max_wait_s = transient_error_max_wait_s
        self._metadata_fetch_timeout_s = metadata_fetch_timeout_s
        if log_transient_errors:
            logging.getLogger("backoff").addHandler(logging.StreamHandler())

    def connect(self) -> None:
        """
        Connects to the Astra database
        """
        tmp_bundle_file_name = str(uuid4())
        os.makedirs(self._tmp_bundle_path, exist_ok=True)

        with open(os.path.join(self._tmp_bundle_path, tmp_bundle_file_name), "wb") as bundle_file:
            bundle_file.write(base64.b64decode(self._secure_connect_bundle_bytes))

        cloud_config = {
            "secure_connect_bundle": os.path.join(self._tmp_bundle_path, tmp_bundle_file_name),
            "connect_timeout": self._metadata_fetch_timeout_s,
        }
        auth_provider = PlainTextAuthProvider(self._client_id, self._client_secret)

        profile = ExecutionProfile(
            retry_policy=RetryPolicy(),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            request_timeout=self._socket_read_timeout / 1e3,
            row_factory=dict_factory,
        )

        # https://docs.datastax.com/en/developer/python-driver/3.28/getting_started/
        self._session = Cluster(
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            cloud=cloud_config,
            auth_provider=auth_provider,
            reconnection_policy=ExponentialReconnectionPolicy(
                self._reconnect_base_delay_ms, self._reconnect_base_delay_ms
            ),
            compression=True,
            application_name=self._client_name,
            application_version=__version__,
            sockopts=[
                (IPPROTO_TCP, TCP_NODELAY, 1),
                (IPPROTO_TCP, TCP_USER_TIMEOUT, self._socket_read_timeout),
            ]
            if platform.system().lower() != "darwin"
            else [(IPPROTO_TCP, TCP_NODELAY, 1)],
        ).connect(self._keyspace)

        set_session(self._session)

        os.remove(os.path.join(self._tmp_bundle_path, tmp_bundle_file_name))

    def disconnect(self) -> None:
        """
        Disconnect from the database and destroy the session.
        """
        self._session.shutdown()
        self._session = None

    def __enter__(self) -> "AstraClient":
        """
        Creates an Astra client for this context.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def get_table_metadata(self, table_name: str) -> TableMetadata:
        """
        Returns Cassandra/Astra table metadata.

        NB. Use the Force, read the Source: https://github.com/datastax/python-driver/blob/master/tests/integration/standard/test_metadata.py#L233-L238

        :param: table_name: Name of the table to read metadata for.
        """
        return get_schema_parser(self._session.cluster.control_connection._connection, "4-a", None, 0.1).get_table(
            keyspaces=None, keyspace=self._keyspace, table=table_name
        )

    def get_entity(self, table_name: str) -> Dict:
        """
        Reads a single row from a table as dictionary
        https://docs.datastax.com/en/developer/python-driver/3.28/cqlengine/queryset/

        :param: table_name: Name of the table to read a row from.
        """

        named_table = NamedTable(self._keyspace, table_name)
        return named_table.objects[0]

    def get_entities_from_query(self, query: str, mapper: Callable[[Dict], TModel]) -> MetaFrame:
        """
        Maps query result to a MetaFrame using custom mapper

        :param: query: A CQL query to execute.
        :param: mapper: A mapping function from a Dictionary to the desired model type.
        """
        return MetaFrame(
            [mapper(entity) for entity in self._session.execute(query)],
            convert_to_polars=polars.DataFrame,
            convert_to_pandas=pandas.DataFrame,
        )

    def filter_entities(
        self,
        model_class: Type[TModel],
        key_column_filter_values: Union[Expression, List[Dict[str, Any]]],
        keyspace: Optional[str] = None,
        table_name: Optional[str] = None,
        select_columns: Optional[List[str]] = None,
        primary_keys: Optional[List[str]] = None,
        partition_keys: Optional[List[str]] = None,
        custom_indexes: Optional[List[str]] = None,
        deduplicate=False,
        num_threads: Optional[int] = None,
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
        def apply(model: Type[Model], key_column_filter: Dict[str, Any], columns_to_select: Optional[List[str]]):
            if columns_to_select:
                return model.filter(**key_column_filter).only(select_columns)

            return model.filter(**key_column_filter)

        def normalize_column_name(column_name: str) -> str:
            filter_suffix = re.findall(self._filter_pattern, column_name)
            if len(filter_suffix) == 0:
                return column_name

            return column_name.replace(filter_suffix[0], "")

        def to_frame(
            model: Type[Model], key_column_filter: Dict[str, Any], columns_to_select: Optional[List[str]]
        ) -> MetaFrame:
            return MetaFrame(
                [dict(v.items()) for v in list(apply(model, key_column_filter, columns_to_select))],
                convert_to_polars=lambda x: polars.DataFrame(x, schema=select_columns),
                convert_to_pandas=lambda x: pandas.DataFrame(x, columns=select_columns),
            )

        assert (
            self._session is not None
        ), "Please instantiate an AstraClient using with AstraClient(...) before calling this method"

        select_columns = list(map(normalize_column_name, select_columns)) if select_columns else None

        cassandra_model = get_mapper(
            data_model=model_class,
            keyspace=keyspace,
            table_name=table_name,
            primary_keys=primary_keys,
            partition_keys=partition_keys,
            custom_indexes=custom_indexes,
        ).map()

        compiled_filter_values = (
            compile_expression(key_column_filter_values, AstraFilterExpression)
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
                    )
                )
        else:
            result = concat(
                [
                    MetaFrame(
                        [dict(v.items()) for v in list(apply(cassandra_model, key_column_filter, select_columns))],
                        convert_to_polars=(lambda x: polars.DataFrame(x, schema=select_columns))
                        if not deduplicate
                        else (lambda x: polars.DataFrame(x, schema=select_columns).unique()),
                        convert_to_pandas=(lambda x: pandas.DataFrame(x, columns=select_columns))
                        if not deduplicate
                        else (lambda x: pandas.DataFrame(x, columns=select_columns).drop_duplicates()),
                    )
                    for key_column_filter in compiled_filter_values
                ]
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

    def delete_entity(self, entity: TModel, table_name: Optional[str] = None, keyspace: Optional[str] = None) -> None:
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
        def _delete_entity(model_class: Type[Model], key_filter: Dict):
            model_class.filter(**key_filter).delete()

        cassandra_model = get_mapper(
            data_model=Type[entity],
            table_name=table_name,
            keyspace=keyspace,
        ).map()

        _delete_entity(
            model_class=cassandra_model,
            key_filter={key: getattr(entity, key) for key in cassandra_model.primary_keys},
        )

    def upsert_entity(
        self,
        entity: TModel,
        keyspace: Optional[str] = None,
        table_name: Optional[str] = None,
        client_rate_limit: str = "1000 per second",
        time_to_live: Optional[int] = None,
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
        entities: List[dict],
        entity_type: Type[TModel],
        keyspace: Optional[str] = None,
        table_name: Optional[str] = None,
        batch_size=1000,
        client_rate_limit: str = "1000 per second",
        time_to_live: Optional[int] = None,
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
        def _save_entities(model_class: Type[Model], values: List[dict], ttl: int):
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

    def ann_search(
        self,
        entity_type: Type[TModel],
        vector_to_match: list[float],
        similarity_function: SimilarityFunction = SimilarityFunction.COSINE,
        key_column_filter_values: Optional[Union[Expression, List[Dict[str, Any]]]] = None,
        table_name: Optional[str] = None,
        return_vector: bool = False,
        num_results=1,
    ) -> MetaFrame:
        """
        Performs a simple ANN-based search for vectors most similar to the provided one in the specified entity. Results are ordered based on similarity metric value.

        Reference CQL code: https://docs.datastax.com/en/astra-serverless/docs/vector-search/cql.html

        References for potential future changes:
           https://github.com/CassioML/cassio/blob/main/src/cassio/utils/vector/distance_metrics.py#L76-L99
           https://github.com/langchain-ai/langchain/blob/93ae589f1bd11f992eff5018660b667b2e15e585/libs/langchain/langchain/vectorstores/cassandra.py
        """

        model_mapper = get_mapper(data_model=entity_type, table_name=table_name)

        query = VectorSearchQuery(
            table_fqn=f"{self._keyspace}.{model_mapper.table_name}",
            data_fields=[f for f in model_mapper.column_names if f != model_mapper.vector_column or return_vector],
            key_column_filter_values=key_column_filter_values,
            sim_func=similarity_function,
            vector=vector_to_match,
            field_name=model_mapper.vector_column,
            num_results=num_results,
        )

        return MetaFrame(
            self._session.execute(str(query)), convert_to_polars=polars.DataFrame, convert_to_pandas=pandas.DataFrame
        )
