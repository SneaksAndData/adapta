"""
 Database client that uses an ODBC driver.
"""

#  Copyright (c) 2023-2024. ECCO Sneaks & Data
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

from abc import ABC
from typing import Optional, Union, Iterator

from pandas import DataFrame, read_sql
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.connectors import pyodbc
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError, OperationalError

from adapta.logs import SemanticLogger
from adapta.storage.database.models import DatabaseType, SqlAlchemyDialect


class OdbcClient(ABC):
    """
    Generic ODBC database client that relies on SqlAlchemy API.
    """

    def __init__(
        self,
        logger: SemanticLogger,
        database_type: DatabaseType,
        host_name: Optional[str] = None,
        user_name: Optional[str] = None,
        database: Optional[str] = None,
        password: Optional[str] = None,
        port: Optional[int] = None,
    ):
        """
         Creates an instance of an OdbcClient

        :param logger: Logger instance for database operations.
        :param database_type: Type of database to connect to.
        :param host_name: Host name.
        :param user_name: SQL user name.
        :param database: Optional database name to connect to.
        :param password: SQL user password.
        :param port: Connection port.
        """
        self._db_type = database_type
        self._dialect: SqlAlchemyDialect = database_type.value
        self._host = host_name
        self._database = database
        self._user = user_name
        self._password = password
        self._port = port
        self._logger = logger
        self._engine = None
        self._connection = None
        pyodbc.pooling = False

    def __enter__(self) -> Optional["OdbcClient"]:
        connection_url: sqlalchemy.engine.URL = URL.create(
            drivername=self._dialect.dialect,
            host=self._host,
            database=self._database,
            username=self._user,
            password=self._password,
            port=self._port,
            query=self._dialect.driver,
        )
        self._logger.info(
            "Connecting to {host}:{port} using dialect {dialect} and driver {driver}",
            host=self._host,
            port=self._port,
            dialect=self._dialect.dialect,
            driver=self._dialect.driver,
        )
        try:
            self._engine: sqlalchemy.engine.Engine = sqlalchemy.create_engine(connection_url, pool_pre_ping=True)
            self._connection: sqlalchemy.engine.Connection = self._engine.connect()
            return self
        except SQLAlchemyError as ex:
            self._logger.error(
                "Error connecting to {host}:{port} using dialect {dialect} and driver {driver}",
                host=self._host,
                port=self._port,
                dialect=self._dialect.dialect,
                driver=self._dialect.driver,
                exception=ex,
            )

            return None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._connection.close()
        self._engine.dispose()

    def fork(self) -> "OdbcClient":
        """
        Copies this client in order to create a new connection, while keeping the other one open (fork).
        """
        return OdbcClient(
            logger=self._logger,
            database_type=self._db_type,
            host_name=self._host,
            user_name=self._user,
            password=self._password,
            port=self._port,
        )

    def _get_connection(self) -> Optional[sqlalchemy.engine.Connection]:
        if self._connection is None:
            self._logger.info("No connection is active. Please create one using with OdbcClient(..) as client: ...")
            return None

        return self._connection

    def query(self, query: str, chunksize: Optional[int] = None) -> Optional[Union[DataFrame, Iterator[DataFrame]]]:
        """
          Read result of SQL query into a pandas dataframe.

        :param query: Query to execute on the connection.
        :param chunksize: Size of an individual data chunk. If not provided, query result will be a single dataframe.
        :return:
        """
        try:
            if chunksize:
                return read_sql(query, con=self._get_connection(), chunksize=chunksize)

            return read_sql(query, con=self._get_connection())
        except SQLAlchemyError as ex:
            self._logger.error("Engine error while executing query {query}", query=query, exception=ex)
            return None
        except BaseException as other:  # pylint: disable=W0703
            self._logger.error(
                "Unknown error while executing query {query}",
                query=query,
                exception=other,
            )
            return None

    def materialize(
        self,
        data: DataFrame,
        schema: str,
        name: str,
        overwrite: bool = False,
        chunksize: Optional[int] = None,
    ) -> Optional[int]:
        """
          Materialize dataframe as a table in a database.

        :param data: Dataframe to materialize as a table.
        :param schema: Schema of a table.
        :param name: Name of a table.
        :param overwrite: Whether to overwrite or append the data.
        :param chunksize: Use this to split a dataframe into chunks and append them sequentially to the target table.
        :return:
        """
        try:
            if overwrite:
                try:
                    if self._dialect.dialect == DatabaseType.SQLITE_ODBC.value.dialect:
                        self._get_connection().execute(text(f"DELETE FROM {schema}.{name}"))
                    else:
                        self._get_connection().execute(text(f"TRUNCATE TABLE {schema}.{name}"))
                except OperationalError as ex:
                    # The table does not exist. Do nothing and let the Pandas API handle the creation of the table.
                    self._logger.warning(
                        "Error truncating {schema}.{table}, now creating table without truncating.",
                        schema=schema,
                        table=name,
                        exception=ex,
                    )

            return data.to_sql(
                name=name,
                schema=schema,
                con=self._get_connection(),
                index=False,
                chunksize=chunksize,
                if_exists="append",
            )
        except SQLAlchemyError as ex:
            self._logger.error(
                "Error while materializing a dataframe into {schema}.{table}",
                schema=schema,
                table=name,
                exception=ex,
            )
            return None
        finally:
            active_tran: sqlalchemy.engine.RootTransaction = self._get_connection().get_transaction()
            if active_tran and active_tran.is_active:
                self._logger.debug(
                    "Found an active transaction for {schema}.{table}. Committing it.",
                    schema=schema,
                    table=name,
                )
                active_tran.commit()
