"""
 QES implementations for Snowflake.
"""
import os
import re
from dataclasses import dataclass
from functools import partial
from typing import final
from collections.abc import Iterator

from dataclasses_json import DataClassJsonMixin

from adapta.storage.database.v3.snowflake_sql import SnowflakeClient
from adapta.storage.models import SnowflakePath
from adapta.storage.models.filter_expression import (
    Expression,
    compile_expression,
    SnowflakeFilterExpression,
)
from adapta.storage.query_enabled_store._models import (
    QueryEnabledStore,
    CONNECTION_STRING_REGEX,
)
from adapta.storage.models.enum import QueryEnabledStoreOptions
from adapta.utils.metaframe import MetaFrame, concat


@dataclass
class SnowflakeCredential(DataClassJsonMixin):
    """
    Snowflake credential helper for QES.

    Snowflake credentials can either be provided via browser SSO, or through (username, password) pair.
    """

    user: str | None = None
    password: str | None = None

    def __post_init__(self):
        self.user = self.user or os.getenv("ADAPTA__SNOWFLAKE_USER")
        self.password = self.password or os.getenv("ADAPTA__SNOWFLAKE_PASSWORD")


@dataclass
class SnowflakeSettings(DataClassJsonMixin):
    """
    Snowflake connection settings for QES.
    """

    account: str | None = None
    warehouse: str | None = None

    def __post_init__(self):
        self.account = self.account or os.getenv("ADAPTA__SNOWFLAKE_ACCOUNT")
        if not self.account:
            raise RuntimeError("Snowflake account not provided.")
        self.warehouse = self.warehouse or os.getenv("ADAPTA__SNOWFLAKE_WAREHOUSE", "AIRFLOW")


@final
class SnowflakeQueryEnabledStore(QueryEnabledStore[SnowflakeCredential, SnowflakeSettings]):
    """
    QES Client for Snowflake queries.

    When password is not None the user and password will be used for login, i.e., `authenticator='snowflake'`
    """

    def close(self) -> None:
        pass

    def __init__(
        self,
        credentials: SnowflakeCredential,
        settings: SnowflakeSettings,
        lazy_init: bool = True,  # pylint: disable=W0613
    ):
        super().__init__(credentials, settings)
        self._snowflake_client = SnowflakeClient(
            user=self.credentials.user,
            account=self.settings.account,
            warehouse=self.settings.warehouse,
            password=self.credentials.password,
        )

    def _apply_filter(
        self,
        path: SnowflakePath,
        filter_expression: Expression,
        columns: list[str],
        options: dict[QueryEnabledStoreOptions, any] | None = None,
        limit: int | None = None,
    ) -> MetaFrame | Iterator[MetaFrame]:
        query_fn = partial(
            self._snowflake_client.query,
            query=self._build_query(
                table_fqn=path.fully_qualified_name, filter_expression=filter_expression, columns=columns, limit=limit
            ),
            batch_size=options.get(QueryEnabledStoreOptions.BATCH_SIZE, 1000),
        )

        with self._snowflake_client:
            return concat(query_fn())

    def _apply_query(self, query: str) -> MetaFrame | Iterator[MetaFrame]:
        raise NotImplementedError("Text queries are not supported by Snowflake QES")

    @classmethod
    def _from_connection_string(
        cls, connection_string: str, lazy_init: bool = False
    ) -> "QueryEnabledStore[SnowflakeCredential, SnowflakeSettings]":
        _, credentials, settings = re.findall(re.compile(CONNECTION_STRING_REGEX), connection_string)[0]
        return cls(
            credentials=SnowflakeCredential.from_json(credentials),
            settings=SnowflakeSettings.from_json(settings),
            lazy_init=lazy_init,
        )

    @staticmethod
    def _build_query(table_fqn: str, filter_expression: Expression, columns: list[str], limit: int | None) -> str:
        """
        Build the final query by applying the filter expression, selected columns, and limit to the base query.
        """

        columns_to_select = ", ".join(columns) if columns else "*"
        query = f"SELECT {columns_to_select} FROM {table_fqn}"

        if filter_expression:
            compiled_expression = compile_expression(expression=filter_expression, target=SnowflakeFilterExpression)
            query = f"{query} WHERE {compiled_expression}"

        if limit:
            query = f"{query} LIMIT {limit}"

        return query
