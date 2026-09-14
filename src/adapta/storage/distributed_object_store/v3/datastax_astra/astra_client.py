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
import shutil
import ssl
import tempfile
import typing
from typing import Any
from uuid import uuid4

from adapta.storage.distributed_object_store.v3.cassandra_client import CassandraClient
from adapta.storage.distributed_object_store.v3.cassandra_client import \
    CassandraClientConfiguration
from adapta.storage.distributed_object_store.v3.cassandra_client import TCassandraModel


import pandas
import polars
from cassandra.auth import PlainTextAuthProvider, AuthProvider

from adapta.storage.distributed_object_store.v3.cassandra_client import (
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

@typing.final
class AstraClient(CassandraClient):
    """
    DataStax Astra (https://astra.datastax.com) credentials provider.

     :param: client_name: Arbitrary string that represents the connecting client in the database.
     :param: keyspace: Keyspace to scope queries to.
     :param: secure_connect_bundle_bytes: Base64-encoded contents (string) of a secure connect bundle.
     :param: client_id: Astra token client_id
     :param: client_secret: Astra token client secret
    """

    def _get_port(self) -> int | None:
        return None

    def _get_contact_points(self) -> list[str] | None:
        return None

    def _get_ssl_context(self) -> ssl.SSLContext | None:
        return None

    def post_connect(self) -> None:
        shutil.rmtree(self._tmp_bundle_path, ignore_errors=True)

    def _cloud_config(self) -> dict | None:
        tmp_bundle_file_name = str(uuid4())
        os.makedirs(self._tmp_bundle_path, exist_ok=True)

        with open(os.path.join(self._tmp_bundle_path, tmp_bundle_file_name), "wb") as bundle_file:
            bundle_file.write(base64.b64decode(self._secure_connect_bundle_bytes))

        return {
            "secure_connect_bundle": os.path.join(self._tmp_bundle_path, tmp_bundle_file_name),
            "connect_timeout": self._client_config.metadata_fetch_timeout_s,
        }

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

    def _get_auth_provider(self) -> AuthProvider:
        return PlainTextAuthProvider(self._client_id, self._client_secret)

    def ann_search(
        self,
        entity_type: type[TCassandraModel],
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
