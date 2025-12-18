"""
 QES implementations for delta-rs.
"""
import re
from dataclasses import dataclass
from pydoc import locate
from typing import final
from collections.abc import Iterator

from dataclasses_json import DataClassJsonMixin

from adapta.security.clients import AuthenticationClient
from adapta.storage.delta_lake.v3 import load
from adapta.storage.models.base import DataPath
from adapta.storage.models.filter_expression import Expression
from adapta.storage.query_enabled_store._models import (
    QueryEnabledStore,
    CONNECTION_STRING_REGEX,
)
from adapta.storage.models.enum import QueryEnabledStoreOptions
from adapta.utils.metaframe import MetaFrame


@dataclass
class DeltaCredential(DataClassJsonMixin):
    """
    Delta-rs credential helper for QES.
    """

    auth_client_class: str
    auth_client_credentials_class: str | None = None

    auth_client: AuthenticationClient | None = None
    auth_client_credentials: type | None = None

    def __post_init__(self):
        if not self.auth_client_class:
            raise ValueError("Authentication plugin class name not provided but is required")

        self.auth_client = locate(self.auth_client_class)

        if self.auth_client is None:
            raise ModuleNotFoundError(
                "Authentication plugin class name cannot be loaded. Please check the spelling and make sure your application can resolve the import"
            )

        if self.auth_client_credentials_class:
            self.auth_client_credentials = locate(self.auth_client_credentials_class)


@dataclass
class DeltaSettings(DataClassJsonMixin):
    """
    Delta QES has no additional settings.
    """


@final
class DeltaQueryEnabledStore(QueryEnabledStore[DeltaCredential, DeltaSettings]):
    """
    QES Client for Delta Lake reads using delta-rs.
    """

    def close(self) -> None:
        pass

    @classmethod
    def _from_connection_string(
        cls, connection_string: str, lazy_init: bool = False
    ) -> "QueryEnabledStore[DeltaCredential, DeltaSettings]":
        _, credentials, settings = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        return cls(credentials=DeltaCredential.from_json(credentials), settings=DeltaSettings.from_json(settings))

    def _apply_filter(
        self,
        path: DataPath,
        filter_expression: Expression,
        columns: list[str],
        options: dict[QueryEnabledStoreOptions, any] | None = None,
        limit: int | None = None,
    ) -> MetaFrame | Iterator[MetaFrame]:
        return load(
            auth_client=self.credentials.auth_client(credentials=self.credentials.auth_client_credentials()),
            path=path,
            row_filter=filter_expression,
            columns=columns if columns else None,
            limit=limit,
            timeout=options.get(QueryEnabledStoreOptions.TIMEOUT, None),
        )

    def _apply_query(self, query: str) -> MetaFrame | Iterator[MetaFrame]:
        raise NotImplementedError("Text queries are not supported by Delta QES")
