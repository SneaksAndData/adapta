"""
 QES implementations for Snowflake.
"""
import os
import re
from dataclasses import dataclass, field
from functools import partial
from typing import final
from collections.abc import Iterator

from dataclasses_json import DataClassJsonMixin, config

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
from adapta.utils.metaframe import MetaFrame


@dataclass
class SnowflakeCredential(DataClassJsonMixin):
    """
    Snowflake credential helper for QES.

    Supports:
      1) browser SSO (no user/password/key fields set),
      2) (username, password) pair,
      3) key-pair auth via ``private_key_file`` (+ optional ``private_key_file_pwd``)
        or a raw ``private_key`` byte string.

    Any unset field falls back to the matching environment variable:
      * ``ADAPTA__SNOWFLAKE_USER``
      * ``ADAPTA__SNOWFLAKE_PASSWORD``
      * ``ADAPTA__SNOWFLAKE_PRIVATE_KEY_FILE``
      * ``ADAPTA__SNOWFLAKE_PRIVATE_KEY_FILE_PWD``
      * ``ADAPTA__SNOWFLAKE_PRIVATE_KEY`` (raw PEM bytes, utf-8 decoded if string)
    """

    user: str | None = None
    password: str | None = None
    private_key_file: str | None = None
    private_key_file_pwd: str | None = None
    # ``dataclasses_json`` cannot decode ``bytes`` from a JSON string by default
    # (it tries ``bytes(<str>)``), so we provide an explicit decoder that turns
    # an incoming PEM string into UTF-8 bytes and leaves ``None``/``bytes`` alone.
    private_key: bytes | None = field(
        default=None,
        metadata=config(decoder=lambda v: v.encode("utf-8") if isinstance(v, str) else v),
    )

    def __post_init__(self):
        self.user = self.user or os.getenv("ADAPTA__SNOWFLAKE_USER")
        self.password = self.password or os.getenv("ADAPTA__SNOWFLAKE_PASSWORD")
        self.private_key_file = self.private_key_file or os.getenv("ADAPTA__SNOWFLAKE_PRIVATE_KEY_FILE")
        self.private_key_file_pwd = self.private_key_file_pwd or os.getenv("ADAPTA__SNOWFLAKE_PRIVATE_KEY_FILE_PWD")

        if self.private_key is None:
            env_key = os.getenv("ADAPTA__SNOWFLAKE_PRIVATE_KEY")
            if env_key is not None:
                self.private_key = env_key.encode("utf-8")
        elif isinstance(self.private_key, str):
            self.private_key = self.private_key.encode("utf-8")


@dataclass
class SnowflakeSettings(DataClassJsonMixin):
    """
    Snowflake connection settings for QES.
    """

    account: str | None = None
    warehouse: str | None = None
    role: str | None = None

    def __post_init__(self):
        self.account = self.account or os.getenv("ADAPTA__SNOWFLAKE_ACCOUNT")
        if not self.account:
            raise RuntimeError("Snowflake account not provided.")
        self.warehouse = self.warehouse or os.getenv("ADAPTA__SNOWFLAKE_WAREHOUSE")
        if not self.warehouse:
            raise RuntimeError("Snowflake warehouse not provided. Required by Adapta SnowflakeClient.")
        self.role = self.role or os.getenv("ADAPTA__SNOWFLAKE_ROLE")


@final
class SnowflakeQueryEnabledStore(QueryEnabledStore[SnowflakeCredential, SnowflakeSettings]):
    """
    QES Client for Snowflake queries.

    When password is not None the user and password will be used for login, i.e., `authenticator='snowflake'`
    """

    def close(self) -> None:
        if not self._lazy:
            self._snowflake_client.disconnect()

    def __init__(
        self,
        credentials: SnowflakeCredential,
        settings: SnowflakeSettings,
        lazy_init: bool = True,
    ):
        super().__init__(credentials, settings)
        self._snowflake_client = SnowflakeClient(
            user=self.credentials.user,
            account=self.settings.account,
            warehouse=self.settings.warehouse,
            password=self.credentials.password,
            role=self.settings.role,
            private_key=self.credentials.private_key,
            private_key_file=self.credentials.private_key_file,
            private_key_file_pwd=self.credentials.private_key_file_pwd,
        )
        self._lazy = lazy_init
        if not lazy_init:
            self._snowflake_client.connect()

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
                inner_query=path.query, filter_expression=filter_expression, columns=columns, limit=limit
            ),
        )

        if self._lazy:
            with self._snowflake_client:
                return query_fn()
        return query_fn()

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
    def _build_query(inner_query: str, filter_expression: Expression, columns: list[str], limit: int | None) -> str:
        """
        Build the final SQL from ``SnowflakePath.query`` (same composition rules as Trino QES).

        If there is no filter, column projection, or limit, ``inner_query`` is returned unchanged.
        Otherwise it is wrapped as ``SELECT ... FROM (inner_query) WHERE ... LIMIT ...``.
        """

        query = inner_query
        if filter_expression or columns or limit:
            columns_to_select = ", ".join(columns) if columns else "*"
            query = f"SELECT {columns_to_select} FROM ({inner_query})"

            if filter_expression:
                compiled_expression = compile_expression(expression=filter_expression, target=SnowflakeFilterExpression)
                query = f"{query} WHERE {compiled_expression}"

            if limit:
                query = f"{query} LIMIT {limit}"

        return query
