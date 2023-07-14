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
import os
import shutil
import tempfile
import uuid
from dataclasses import fields
from typing import Optional, Dict, TypeVar, Callable, Any, Type, List

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
from cassandra.policies import ExponentialReconnectionPolicy
from cassandra.query import dict_factory  # pylint: disable=E0611

from adapta import __version__

TModel = TypeVar("TModel")  # pylint: disable=C0103


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


class AstraClient:
    """
    DataStax Astra (https://astra.datastax.com) credentials provider.

    EXPERIMENTAL API NOTICE: THIS API IS NOT FINALIZED AND IS A SUBJECT TO CHANGE. USE WITH CAUTION UNTIL IT GRADUATES TO STABLE.
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

    def __enter__(self) -> "AstraClient":
        """
        Creates an Astra client for this context.

        :param: reconnect_base_delay_ms: Reconnect delay in ms (min value for exp. backoff).
        :param: reconnect_max_delay_ms: Reconnect delay in ms (max value for exp backoff).
        :param: socket_connection_timeout: Connect timeout for TCP operations.
        :param: socket_read_timeout: Read timeout for TCP operations (query timeout).
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

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._session.shutdown()
        self._session = None
        shutil.rmtree(self._tmp_bundle_path)

    def get_entity(self, table_name: str) -> Dict:
        """
        Reads a single row from a table as dictionary
        https://docs.datastax.com/en/developer/python-driver/3.28/cqlengine/queryset/
        """

        named_table = NamedTable(self._keyspace, table_name)
        return named_table.objects[0]

    def get_entities(self, query: str, mapper: Callable[[Dict], TModel]) -> pandas.DataFrame:
        """
        Maps query result to a pandas Dataframe using custom mapper
        """
        return pandas.DataFrame([mapper(entity) for entity in self._session.execute(query)])

    def get_entities_raw(self, query: str) -> pandas.DataFrame:
        """
        Maps query result to a pandas Dataframe
        """
        return pandas.DataFrame(self._session.execute(query))

    @staticmethod
    def model_dataclass(value: Any, primary_keys: List[str]):
        """
        Maps a Python dataclass to Cassandra model.
        """

        def map_to_cassandra(python_type: Type, is_primary_key: bool) -> Column:
            if python_type is str:
                return columns.Text(partition_key=is_primary_key)
            if python_type is int:
                return columns.Integer(partition_key=is_primary_key)
            if python_type is float:
                return columns.Double(partition_key=is_primary_key)
            if python_type is List[str]:
                return columns.List(partition_key=False, value_type=columns.Text)

            raise TypeError(f"Unsupported type: {python_type}")

        models_attributes: Dict[str, Column] = {
            field.name: map_to_cassandra(field.type, field.name in primary_keys) for field in fields(value)
        }
        return type(f"{type(value)}Model", (Model,), models_attributes)
