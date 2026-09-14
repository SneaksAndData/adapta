"""
DataStax Astra client driver.
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

import base64
import logging
import os
import platform
import tempfile
import typing
from typing import Any, TypeVar
from uuid import uuid4

from adapta.storage.distributed_object_store.v3.cassandra_client import CassandraClient
from adapta.storage.distributed_object_store.v3.cassandra_client import \
    CassandraClientConfiguration

try:
    from _socket import IPPROTO_TCP, TCP_NODELAY, TCP_USER_TIMEOUT
except ImportError:
    # Fix for MacOS - MacOS does not have TCP_USER_TIMEOUT as in linux _socket module - https://man7.org/linux/man-pages/man7/tcp.7.html
    # So we removed TCP_USER_TIMEOUT from _socket import
    from socket import IPPROTO_TCP, TCP_NODELAY

import pandas
import polars
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (  # pylint: disable=E0611
    EXEC_PROFILE_DEFAULT,
    Cluster,
    ExecutionProfile,
    RetryPolicy,
)
from cassandra.cqlengine.connection import set_session
from cassandra.policies import ExponentialReconnectionPolicy
from cassandra.query import dict_factory  # pylint: disable=E0611

from adapta import __version__
from adapta.storage.distributed_object_store.v3.datastax_astra._model_mappers import (
    get_mapper,
)
from adapta.storage.distributed_object_store.v3.datastax_astra._models import (
    SimilarityFunction,
    VectorSearchQuery,
)
from adapta.storage.models.expression_dsl.filter_expression import (
    Expression,
)
from adapta.utils.metaframe import MetaFrame

TModel = TypeVar("TModel")  # pylint: disable=C0103


@typing.final
class AstraClient(CassandraClient):
    """
    DataStax Astra (https://astra.datastax.com) credentials provider.

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
     :param: protocol_version: Cassandra protocol version to use. Defaults to the latest version supported by the driver.
    """

    async def connect_async(self) -> None:
        raise NotImplementedError("Unsupported")

    # pylint: disable=too-many-locals
    def __init__(self, client_name: str, keyspace: str | None = None, secure_connect_bundle_bytes: str | None = None,
                 client_id: str | None = None, client_secret: str | None = None, client_config: CassandraClientConfiguration | None = None):
        super().__init__(
            client_name=client_name,
            keyspace=keyspace,
            client_config=client_config
        )
        self._secure_connect_bundle_bytes = secure_connect_bundle_bytes or os.getenv("PROTEUS__ASTRA_BUNDLE_BYTES")
        self._client_id = client_id or os.getenv("PROTEUS__ASTRA_CLIENT_ID")
        self._client_secret = client_secret or os.getenv("PROTEUS__ASTRA_CLIENT_SECRET")
        self._tmp_bundle_path = os.path.join(tempfile.gettempdir(), ".astra")

        if self._client_config.log_transient_errors:
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
            "connect_timeout": self._client_config.metadata_fetch_timeout_ms,
        }
        auth_provider = PlainTextAuthProvider(self._client_id, self._client_secret)

        profile = ExecutionProfile(
            retry_policy=RetryPolicy(),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            request_timeout=self._client_config.socket_read_timeout_ms / 1e3,
            row_factory=dict_factory,
        )

        # https://docs.datastax.com/en/developer/python-driver/3.28/getting_started/
        self._cluster = Cluster(
            connection_class=self._client_config.connection_class,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            cloud=cloud_config,
            auth_provider=auth_provider,
            reconnection_policy=ExponentialReconnectionPolicy(
                self._client_config.reconnect_base_delay_ms, self._client_config.reconnect_max_delay_ms
            ),
            compression=True,
            application_name=self._client_name,
            application_version=__version__,
            protocol_version=self._client_config.protocol_version,
            sockopts=[
                (IPPROTO_TCP, TCP_NODELAY, 1),
                (IPPROTO_TCP, TCP_USER_TIMEOUT, self._client_config.socket_read_timeout_ms),
            ]
            if platform.system().lower() != "darwin"
            else [(IPPROTO_TCP, TCP_NODELAY, 1)],
        )

        self._session = self._cluster.connect(self._keyspace)

        set_session(self._session)

        os.remove(os.path.join(self._tmp_bundle_path, tmp_bundle_file_name))

    def ann_search(
        self,
        entity_type: type[TModel],
        vector_to_match: list[float],
        similarity_function: SimilarityFunction = SimilarityFunction.COSINE,
        key_column_filter_values: Expression | list[dict[str, Any]] | None = None,
        table_name: str | None = None,
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
