import os
import re
from dataclasses import dataclass, field
from typing import final, Optional

from adapta._version import __version__

from dataclasses_json import DataClassJsonMixin

from adapta.storage.query_enabled._models import QueryEnabledStoreConnection, CONNECTION_STRING_REGEX


@dataclass
class AstraCredential(DataClassJsonMixin):
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
    client_name: Optional[str] = None
    keyspace: Optional[str] = None

    def __post_init__(self):
        self.client_name = self.client_name or f"Adapta Client {__version__}"
        self.keyspace = self.keyspace or os.getenv("PROTEUS__ASTRA_KEYSPACE")


@final
class AstraQes(QueryEnabledStoreConnection[AstraCredential, AstraSettings]):
    @classmethod
    def _from_connection_string(
        cls, connection_string: str
    ) -> "QueryEnabledStoreConnection[AstraCredential, AstraSettings]":
        _, credentials, settings = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        return cls(credentials=AstraCredential.from_json(credentials), settings=AstraSettings.from_json(settings))
