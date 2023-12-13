"""
 QES implementations for DataStax Astra.
"""
import os
import re
from dataclasses import dataclass
from typing import final, Optional, Union, Iterator

import pandas

from dataclasses_json import DataClassJsonMixin

from adapta._version import __version__
from adapta.storage.distributed_object_store.datastax_astra.astra_client import AstraClient
from adapta.storage.models.astra import AstraPath
from adapta.storage.models.base import DataPath
from adapta.storage.models.filter_expression import Expression

from adapta.storage.query_enabled._models import QueryEnabledStore, CONNECTION_STRING_REGEX


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
class AstraQes(QueryEnabledStore[AstraCredential, AstraSettings]):
    """
    QES Client for Astra DB (Cassandra).
    """

    def _apply_filter(
        self, path: DataPath, filter_expression: Expression, columns: list[str]
    ) -> Union[pandas.DataFrame, Iterator[pandas.DataFrame]]:
        assert isinstance(path, AstraPath)
        astra_path: AstraPath = path

        with AstraClient(
            client_name=self.settings.client_name,
            keyspace=astra_path.keyspace,
            secure_connect_bundle_bytes=self.credentials.secret_connection_bundle_bytes,
            client_id=self.credentials.client_id,
            client_secret=self.credentials.client_secret,
        ) as astra_client:
            return astra_client.filter_entities(
                model_class=astra_path.model_class(),
                key_column_filter_values=filter_expression,
                table_name=astra_path.table,
                select_columns=columns,
            )

    def _apply_query(self, query: str) -> Union[pandas.DataFrame, Iterator[pandas.DataFrame]]:
        with AstraClient(
            client_name=self.settings.client_name,
            keyspace=self.settings.keyspace,
            secure_connect_bundle_bytes=self.credentials.secret_connection_bundle_bytes,
            client_id=self.credentials.client_id,
            client_secret=self.credentials.client_secret,
        ) as astra_client:
            return astra_client.get_entities_raw(query)

    @classmethod
    def _from_connection_string(cls, connection_string: str) -> "QueryEnabledStore[AstraCredential, AstraSettings]":
        _, credentials, settings = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        return cls(credentials=AstraCredential.from_json(credentials), settings=AstraSettings.from_json(settings))
