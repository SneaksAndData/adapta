"""
  SqlAlchemy-based Trino Client Wrapper
"""

import os
from typing import Optional, Iterator

import pandas
import sqlalchemy.engine
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from trino.auth import OAuth2Authentication

from proteus.logs.models import LogLevel
from proteus.logs import ProteusLogger
from proteus.storage.secrets import SecretStorageClient


class TrinoClient:
    """
      Trino (https://www.trino.io) connection client.
    """

    def __init__(
            self,
            host: str,
            catalog: str,
            port: Optional[int] = 443,
            oauth2_username: Optional[str] = None,
            credentials_provider: Optional[(str, SecretStorageClient)] = None,
            logger: ProteusLogger = ProteusLogger().add_log_source(
                log_source_name='proteus-trino-client',
                min_log_level=LogLevel.INFO,
                is_default=True
            )
    ):
        """
         Initializes a SqlAlchemy Engine that will facilitate connections to Trino.

        :param host: Trino Coordinator hostname, without protocol.
        :param catalog: Trino catalog.
        :param port: Trino connection port (443 default).
        :param oauth2_username: Optional username to use if authenticating with interactive OAuth2.
               Can also be provided via PROTEUS__TRINO_OAUTH2_USERNAME.
        :param credentials_provider: Optional secret provider to use to read Basic Auth credentials.
        :param logger: Proteus logger instance.
        """

        self._host = host
        self._catalog = catalog
        self._port = port
        if 'PROTEUS__TRINO_USERNAME' in os.environ:
            self._engine = create_engine(
                f"trino://{os.getenv('PROTEUS__TRINO_USERNAME')}:{os.getenv('PROTEUS__TRINO_PASSWORD')}@{self._host}:{self._port}/{self._catalog}")
        elif 'PROTEUS__TRINO_OAUTH2_USERNAME' in os.environ or oauth2_username:
            self._engine = create_engine(
                f"trino://{os.getenv('PROTEUS__TRINO_OAUTH2_USERNAME')}@{self._host}:{self._port}/{self._catalog}",
                connect_args={
                    "auth": OAuth2Authentication(),
                    "http_scheme": "https",
                }
            )
        elif credentials_provider:
            credentials_secret = credentials_provider[1].read_secret('', credentials_provider[0])
            self._engine = create_engine(
                f"trino://{credentials_secret['username']}:{credentials_secret['password']}@{self._host}:{self._port}/{self._catalog}")
        else:
            raise ConnectionError('Neither PROTEUS__TRINO_USERNAME or PROTEUS__TRINO_OAUTH2_USERNAME is specified. Cannot authenticate to the provided host.')

        self._logger = logger
        self._connection: Optional[sqlalchemy.engine.Connection] = None

    def __enter__(self) -> Optional['TrinoClient']:
        try:
            self._connection = self._engine.connect()
            return self
        except SQLAlchemyError as ex:
            self._logger.error(
                'Error connecting to {host}:{port}',
                host=self._host,
                port=self._port,
                exception=ex
            )
            return None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._connection.close()
        self._engine.dispose()

    def query(
            self,
            query: str,
            batch_size: int = 1000
    ) -> Iterator[pandas.DataFrame]:
        """
          Executes a Trino DML query and converts the result into a Pandas dataframe.

          This method internally calls pandas.read_sql_query

          :param query: SQL query compliant with https://trino.io/docs/current/sql.html
          :param batch_size: Optional batch size to return rows iteratively.
        """

        return pandas.read_sql_query(
            sql=query,
            con=self._connection,
            chunksize=batch_size
        )
