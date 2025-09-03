"""
 QES implementations for Trino.
"""
import os
import re
from dataclasses import dataclass
from typing import final
from collections.abc import Iterator

from dataclasses_json import DataClassJsonMixin

from adapta.storage.database.v3.trino_sql import TrinoClient
from adapta.storage.models import TrinoPath
from adapta.storage.models.filter_expression import Expression, compile_expression, ArrowFilterExpression
from adapta.storage.query_enabled_store._models import (
    QueryEnabledStore,
    CONNECTION_STRING_REGEX,
)
from adapta.storage.models.enum import QueryEnabledStoreOptions
from adapta.utils.metaframe import MetaFrame, concat


@dataclass
class TrinoCredential(DataClassJsonMixin):
    """
    Trino credential helper for QES.

    Trino credentials can either be provided via the oauth2_username or via the following environment variables, which
    is handled inside the TrinoClient:
    - (ADAPTA__TRINO_USERNAME, ADAPTA__TRINO_PASSWORD)

    Currently, we don't support the credentials_provider option of the TrinoClient.
    """

    oauth2_username: str | None = None

    def __post_init__(self):
        self.oauth2_username = self.oauth2_username or os.getenv("ADAPTA__TRINO_OAUTH2_USERNAME")


@dataclass
class TrinoSettings(DataClassJsonMixin):
    """
    Trino connection settings for QES.
    """

    host: str | None = None
    port: int | None = None

    def __post_init__(self):
        self.host = self.host or os.getenv("PROTEUS__TRINO_HOST")
        if not self.host:
            raise RuntimeError(
                "Trino host not provided. Please provide it via connection string or via environment variable PROTEUS__TRINO_HOST."
            )
        self.port = self.port or int(os.getenv("PROTEUS__TRINO_PORT", "443"))


@final
class TrinoQueryEnabledStore(QueryEnabledStore[TrinoCredential, TrinoSettings]):
    """
    QES Client for Trino queries.
    """

    def close(self) -> None:
        pass

    def __init__(self, credentials: TrinoCredential, settings: TrinoSettings):
        super().__init__(credentials, settings)
        self._trino_client = TrinoClient(
            host=self.settings.host,
            port=self.settings.port,
            oauth2_username=self.credentials.oauth2_username,
        )

    def _apply_filter(
        self,
        path: TrinoPath,
        filter_expression: Expression,
        columns: list[str],
        options: dict[QueryEnabledStoreOptions, any] | None = None,
        limit: int | None = None,
    ) -> MetaFrame | Iterator[MetaFrame]:
        with self._trino_client as trino_client:
            if QueryEnabledStoreOptions.BATCH_SIZE in options:
                data = concat(
                    trino_client.query(
                        query=path.query,
                        batch_size=options[QueryEnabledStoreOptions.BATCH_SIZE],
                    )
                )
            else:
                data = concat(trino_client.query(query=path.query))

        if limit or columns or filter_expression:
            pyarrow_data = data.to_polars().to_arrow()

            if filter_expression:
                row_filter = (
                    compile_expression(filter_expression, ArrowFilterExpression)
                    if isinstance(filter_expression, Expression)
                    else filter_expression
                )
                pyarrow_data = pyarrow_data.filter(row_filter)

            if columns:
                pyarrow_data = pyarrow_data.select(columns)

            if limit:
                pyarrow_data = pyarrow_data.slice(0, limit)

            data = MetaFrame.from_arrow(pyarrow_data)

        return data

    def _apply_query(self, query: str) -> MetaFrame | Iterator[MetaFrame]:
        raise NotImplementedError("Text queries are not supported by Trino QES")

    @classmethod
    def _from_connection_string(
        cls, connection_string: str, lazy_init: bool = False
    ) -> "QueryEnabledStore[TrinoCredential, TrinoSettings]":
        _, credentials, settings = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        return cls(
            credentials=TrinoCredential.from_json(credentials),
            settings=TrinoSettings.from_json(settings),
        )
