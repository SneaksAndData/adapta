import re
from dataclasses import dataclass
from typing import final

from dataclasses_json import DataClassJsonMixin

from adapta.storage.query_enabled._models import QueryEnabledStoreConnection, CONNECTION_STRING_REGEX


@dataclass
class DeltaCredential(DataClassJsonMixin):
    auth_client_class: str

    def __post_init__(self):
        if not self.auth_client_class:
            raise ValueError("Authentication plugin class name not provided but is required")


@dataclass
class DeltaSettings(DataClassJsonMixin):
    account: str
    container: str

    def __post_init__(self):
        if not self.account or not self.container:
            raise ValueError("Authentication plugin requires both account and container value to be set")


@final
class DeltaQes(QueryEnabledStoreConnection[DeltaCredential, DeltaSettings]):
    @classmethod
    def _from_connection_string(
        cls, connection_string: str
    ) -> "QueryEnabledStoreConnection[DeltaCredential, DeltaSettings]":
        _, credentials, settings = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        return cls(credentials=DeltaCredential.from_json(credentials), settings=DeltaSettings.from_json(settings))
