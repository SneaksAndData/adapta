"""
 DataStax Astra client driver.
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

import base64
import datetime
import os
import re
import shutil
import tempfile
import uuid
from dataclasses import fields, is_dataclass
from types import NoneType
from typing import Optional, Dict, TypeVar, Callable, Type, List

from _socket import IPPROTO_TCP, TCP_NODELAY, TCP_USER_TIMEOUT

import pandas
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (  # pylint: disable=E0611
    Cluster,
    Session,
    RetryPolicy,
    ExecutionProfile,
    EXEC_PROFILE_DEFAULT,
)
from cassandra.cqlengine import columns
from cassandra.cqlengine.columns import Column
from cassandra.cqlengine.connection import set_session
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.named import NamedTable
from cassandra.metadata import TableMetadata, get_schema_parser  # pylint: disable=E0611
from cassandra.policies import ExponentialReconnectionPolicy
from cassandra.query import dict_factory  # pylint: disable=E0611

from adapta import __version__

TModel = TypeVar("TModel")  # pylint: disable=C0103


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
    """

    def __init__(
        self,
        client_name: str,
        keyspace: str,
        secure_connect_bundle_bytes: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        reconnect_base_delay_ms=1000,
        reconnect_max_delay_ms=5000,
        socket_connection_timeout=5000,
        socket_read_timeout=180000,
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
        self._socket_connection_timeout = socket_connection_timeout
        self._socket_read_timeout = socket_read_timeout
        self._query_timeout = socket_read_timeout
        self._snake_pattern = re.compile(r"(?<!^)(?=[A-Z])")

    def __enter__(self) -> "AstraClient":
        """
        Creates an Astra client for this context.
        """
        tmp_bundle_file_name = str(uuid.uuid4())
        os.makedirs(self._tmp_bundle_path, exist_ok=True)

        with open(os.path.join(self._tmp_bundle_path, tmp_bundle_file_name), "wb") as bundle_file:
            bundle_file.write(base64.b64decode(self._secure_connect_bundle_bytes))

        cloud_config = {"secure_connect_bundle": os.path.join(self._tmp_bundle_path, tmp_bundle_file_name)}
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
            ],
        ).connect(self._keyspace)

        set_session(self._session)

        shutil.rmtree(self._tmp_bundle_path)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.shutdown()
        self._session = None

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

    def get_entities_from_query(self, query: str, mapper: Callable[[Dict], TModel]) -> pandas.DataFrame:
        """
        Maps query result to a pandas Dataframe using custom mapper

        :param: query: A CQL query to execute.
        :param: mapper: A mapping function from a Dictionary to the desired model type.
        """
        return pandas.DataFrame([mapper(entity) for entity in self._session.execute(query)])

    def filter_entities(
        self,
        model_class: TModel,
        key_column_filter_values: List[Dict[str, str]],
        table_name: Optional[str] = None,
        primary_keys: Optional[List[str]] = None,
        partition_keys: Optional[List[str]] = None,
        deduplicate=False,
    ) -> pandas.DataFrame:
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
        :param: table_name: Optional Astra table name, if it cannot be inferred from class name by converting it to snake_case.
        :param: primary_keys: An optional list of columns that constitute a primary key, if it cannot be inferred from is_primary_key metadata on a dataclass field.
        :param: partition_keys: An optional list of columns that constitute a partition key, if it cannot be inferred from is_partition_key metadata on a dataclass field.
        param: deduplicate: Optionally deduplicate query result, for example when only the partition key part of a primary key is used to fetch results.
        """
        assert (
            self._session is not None
        ), "Please instantiate an AstraClient using with AstraClient(...) before calling this method"

        model_class: Model = self._model_dataclass(model_class, table_name, primary_keys, partition_keys)
        result = pandas.concat(
            [
                pandas.DataFrame([dict(v.items()) for v in list(model_class.filter(**key_column_filter))])
                for key_column_filter in key_column_filter_values
            ]
        )

        if deduplicate:
            return result.drop_duplicates()

        return result

    def get_entities_raw(self, query: str) -> pandas.DataFrame:
        """
         Maps query result to a pandas Dataframe

        :param: query: A CQL query to run.
        """
        return pandas.DataFrame(self._session.execute(query))

    def _model_dataclass(
        self,
        value: Type,
        table_name: Optional[str] = None,
        primary_keys: Optional[List[str]] = None,
        partition_keys: Optional[List[str]] = None,
    ) -> Model:
        """
        Maps a Python dataclass to Cassandra model.

        :param: value: A dataclass type that should be mapped to Astra Model.
        :param: table_name: Astra table name, if it cannot be inferred from class name by converting it to snake_case.
        :param: primary_keys: An optional list of columns that constitute a primary key, if it cannot be inferred from is_primary_key metadata on a dataclass field.
        :param: partition_keys: An optional list of columns that constitute a partition key, if it cannot be inferred from is_partition_key metadata on a dataclass field.
        """

        def map_to_cassandra(  # pylint: disable=R0911
            python_type: Type, db_field: str, is_primary_key: bool, is_partition_key: bool
        ) -> Column:
            if python_type is NoneType:
                raise TypeError("NoneType cannot be mapped to any existing table column types")
            if python_type is bool:
                return columns.Boolean(primary_key=is_primary_key, partition_key=is_partition_key, db_field=db_field)
            if python_type is str:
                return columns.Text(primary_key=is_primary_key, partition_key=is_partition_key, db_field=db_field)
            if python_type is bytes:
                return columns.Blob(primary_key=is_primary_key, partition_key=is_partition_key, db_field=db_field)
            if python_type is datetime.datetime:
                return columns.DateTime(primary_key=is_primary_key, partition_key=is_partition_key, db_field=db_field)
            if python_type is int:
                return columns.Integer(primary_key=is_primary_key, partition_key=is_partition_key, db_field=db_field)
            if python_type is float:
                return columns.Double(primary_key=is_primary_key, partition_key=is_partition_key, db_field=db_field)
            if python_type is List[str]:
                return columns.List(primary_key=False, partition_key=False, value_type=columns.Text, db_field=db_field)
            if python_type is List[int]:
                return columns.List(
                    primary_key=False, partition_key=False, value_type=columns.Integer, db_field=db_field
                )
            if python_type is List[float]:
                return columns.List(
                    primary_key=False, partition_key=False, value_type=columns.Double, db_field=db_field
                )
            if python_type is Dict[str, str]:
                return columns.Map(
                    primary_key=False,
                    partition_key=False,
                    key_type=columns.Text,
                    value_type=columns.Text,
                    db_field=db_field,
                )

            raise TypeError(f"Unsupported type: {python_type}")

        assert is_dataclass(value)

        primary_keys = primary_keys or [
            field.name for field in fields(value) if field.metadata.get("is_primary_key", False)
        ]
        partition_keys = partition_keys or [
            field.name for field in fields(value) if field.metadata.get("is_partition_key", False)
        ]

        table_name = table_name or self._snake_pattern.sub("_", table_name).lower()

        models_attributes: Dict[str, Column] = {
            field.name: map_to_cassandra(
                field.type, field.name, field.name in primary_keys, field.name in partition_keys
            )
            for field in fields(value)
        }

        return type(table_name, (Model,), models_attributes)

    def set_table_option(self, table_name: str, option_name: str, option_value: str) -> None:
        """
        Sets a table property: https://docs.datastax.com/en/cql-oss/3.1/cql/cql_reference/tabProp.html

        :param: table_name: Table to set property for.
        :param: option_name: Table option to set value for.
        :param: option_value: Table option value to set.
        """
        self._session.execute(f"ALTER TABLE {self._keyspace}.{table_name} with {option_name}={option_value};")
