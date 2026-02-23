"""
  SqlAlchemy-based Trino Client Wrapper
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

import os
from dataclasses import dataclass
from typing import final, Optional, Self
from collections.abc import Iterator

import sqlalchemy
from pandas import read_sql_query
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from trino.auth import OAuth2Authentication, BasicAuthentication

from adapta.logs.models import LogLevel
from adapta.logs import SemanticLogger
from adapta.storage.secrets import SecretStorageClient
from adapta.utils.metaframe import MetaFrame


@final
@dataclass
class TrinoConnectionSecret:
    """
    Connection secret structure for Trino
    """

    secret_name: str
    username_secret_key: str
    password_secret_key: str


class TrinoClient:
    """
    Trino (https://www.trino.io) connection client.
    """

    def __init__(
        self,
        host: str,
        catalog: str | None = None,
        port: int | None = 443,
        oauth2_username: str | None = None,
        username: str | None = None,
        password: str | None = None,
        credentials_provider: tuple[TrinoConnectionSecret, SecretStorageClient] | None = None,
        logger: SemanticLogger = SemanticLogger().add_log_source(
            log_source_name="adapta-trino-client",
            min_log_level=LogLevel.INFO,
            is_default=True,
        ),
    ):
        """
         Initializes a SqlAlchemy Engine that will facilitate connections to Trino.
         Authentication options:
          - via OAuth2 if oauth2_username or ADAPTA__TRINO_OAUTH2_USERNAME is provided
          - via external secret provider (Vault, Azure KeyVault, AWS Secrets Manager, etc.) if credentials_provider is provided
          - via plaintext username-password if ADAPTA__TRINO_USERNAME and ADAPTA__TRINO_PASSWORD are provided
          - via plaintext username-password if username and password parameters are provided directly

        :param host: Trino Coordinator hostname, without protocol.
        :param catalog: Trino catalog.
        :param port: Trino connection port (443 default).
        :param oauth2_username: Optional username to use if authenticating with interactive OAuth2.
               Can also be provided via ADAPTA__TRINO_OAUTH2_USERNAME.
        :param username: Optional username to use if authenticating with Basic Auth.
               Can also be provided via ADAPTA__TRINO_USERNAME.
        :param password: Optional password to use if authenticating with Basic Auth.
               Can also be provided via ADAPTA__TRINO_PASSWORD.
        :param credentials_provider: Optional secret provider and auth secret details to use to read Basic Auth credentials.
        :param logger: CompositeLogger instance.
        """

        self._host = host
        self._catalog = catalog
        self._port = port

        username = username or os.getenv("ADAPTA__TRINO_USERNAME", None)
        if username:
            password = password or os.getenv("ADAPTA__TRINO_PASSWORD", None)
            self._engine = create_engine(
                f"trino://{username}@{self._host}:{self._port}/{self._catalog or ''}",
                connect_args={
                    "auth": BasicAuthentication(username, password),
                    "http_scheme": "https",
                },
            )
        elif "ADAPTA__TRINO_OAUTH2_USERNAME" in os.environ or oauth2_username:
            self._engine = create_engine(
                f"trino://{os.getenv('ADAPTA__TRINO_OAUTH2_USERNAME')}@{self._host}:{self._port}/{self._catalog or ''}",
                connect_args={
                    "auth": OAuth2Authentication(),
                    "http_scheme": "https",
                },
            )
        elif credentials_provider:
            credentials_secret = credentials_provider[1].read_secret("", credentials_provider[0].secret_name)
            username = credentials_secret[credentials_provider[0].username_secret_key]
            self._engine = create_engine(
                f"trino://{username}@{self._host}:{self._port}/{self._catalog or ''}",
                connect_args={
                    "auth": BasicAuthentication(
                        username, credentials_secret[credentials_provider[0].password_secret_key]
                    ),
                    "http_scheme": "https",
                },
            )
        else:
            raise ConnectionError(
                "Neither ADAPTA__TRINO_USERNAME or ADAPTA__TRINO_OAUTH2_USERNAME is specified. Cannot authenticate to the provided host."
            )

        self._logger = logger
        self._connection: sqlalchemy.engine.Connection | None = None

    def connect(self) -> None:
        """
        Establishes connection to Trino. This method is implicitly called when using the client as a context manager.
        """
        if self._connection:
            self._logger.info(
                "Connection to {host}:{port} is already established.",
                host=self._host,
                port=self._port,
            )
            return

        self._connection = self._engine.connect()

    def disconnect(self) -> None:
        """
        Closes the connection to Trino. This method is implicitly called when using the client as a context manager.
        """

        if self._connection:
            self._connection.close()
            self._connection = None

        self._engine.dispose()
        self._engine = None

    def __enter__(self) -> Self:
        try:
            if not self._connection:
                self._connection = self._engine.connect()
            else:
                self._logger.info(
                    "Connection to {host}:{port} is already established.",
                    host=self._host,
                    port=self._port,
                )
            return self
        except SQLAlchemyError as ex:
            self._logger.error(
                "Error connecting to {host}:{port}",
                host=self._host,
                port=self._port,
                exception=ex,
            )
            return None

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            self._connection.close()
        except SQLAlchemyError as ex:
            self._logger.warning(
                "Error closing connection to {host}:{port}",
                host=self._host,
                port=self._port,
                exception=ex,
            )
        try:
            self._engine.dispose()
        except SQLAlchemyError as ex:
            self._logger.warning(
                "Error disposing engine for {host}:{port}",
                host=self._host,
                port=self._port,
                exception=ex,
            )

    def query(self, query: str, batch_size: int | None = 1000) -> Iterator[MetaFrame]:
        """
        Executes a Trino DML query and converts the result into a Pandas dataframe.

        This method internally calls pandas.read_sql_query

        :param query: SQL query compliant with https://trino.io/docs/current/sql.html
        :param batch_size: Optional batch size to return rows iteratively.
        """

        return (
            MetaFrame.from_pandas(chunk)
            for chunk in read_sql_query(sql=query, con=self._connection, chunksize=batch_size)
        )
