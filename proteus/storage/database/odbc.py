"""
 Database client that uses an ODBC driver.
"""

from abc import ABC
from typing import Optional, Union, Iterator

import pandas
import sqlalchemy
from sqlalchemy.connectors import pyodbc
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError

from proteus.logs import ProteusLogger
from proteus.storage.database.models import DatabaseType
from proteus.storage.database.models import SqlAlchemyDialect


class OdbcClient(ABC):
    """
     Generic ODBC database client that relies on SqlAlchemy API.
    """
    def __init__(
            self,
            logger: ProteusLogger,
            database_type: DatabaseType,
            host_name: Optional[str] = None,
            user_name: Optional[str] = None,
            database: Optional[str] = None,
            password: Optional[str] = None,
            port: Optional[int] = None
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

    def __enter__(self):
        connection_url: sqlalchemy.engine.URL = URL.create(
            drivername=self._dialect.dialect,
            host=self._host,
            database=self._database,
            username=self._user,
            password=self._password,
            port=self._port,
            query=self._dialect.driver
        )
        self._logger.info(
            'Connecting to {host}:{port} using dialect {dialect} and driver {driver}',
            host=self._host,
            port=self._port,
            dialect=self._dialect.dialect,
            driver=self._dialect.driver
        )
        try:
            self._engine: sqlalchemy.engine.Engine = sqlalchemy.create_engine(connection_url, pool_pre_ping=True)
            self._connection: sqlalchemy.engine.Connection = self._engine.connect()
        except SQLAlchemyError as ex:
            self._logger.error(
                'Error connecting to {host}:{port} using dialect {dialect} and driver {driver}',
                host=self._host,
                port=self._port,
                dialect=self._dialect.dialect,
                driver=self._dialect.driver,
                exception=ex
            )

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._connection.close()
        self._engine.dispose()

    def fork(self) -> 'OdbcClient':
        """
         Copies this client in order to create a new connection, while keeping the other one open (fork).
        """
        return OdbcClient(
            logger=self._logger,
            database_type=self._db_type,
            host_name=self._host,
            user_name=self._user,
            password=self._password,
            port=self._port
        )

    def _get_connection(self) -> Optional[sqlalchemy.engine.Connection]:
        if self._connection is None:
            self._logger.info('No connection is active. Please create one using with OdbcClient(..) as client: ...')
            return None

        return self._connection

    def query(
            self,
            query: str,
            chunksize: Optional[int] = None
    ) -> Optional[Union[pandas.DataFrame, Iterator[pandas.DataFrame]]]:
        """
          Read result of SQL query into a pandas dataframe.

        :param query: Query to execute on the connection.
        :param chunksize: Size of an individual data chunk. If not provided, query result will be a single dataframe.
        :return:
        """
        try:
            if chunksize:
                return pandas.read_sql(query, con=self._get_connection(), chunksize=chunksize)

            return pandas.read_sql(query, con=self._get_connection())
        except SQLAlchemyError as ex:
            self._logger.error('Engine error while executing query {query}', query=query, exception=ex)
            return None
        except BaseException as other:  # pylint: disable=W0703
            self._logger.error('Unknown error while executing query {query}', query=query, exception=other)
            return None

    def materialize(
            self,
            data: pandas.DataFrame,
            schema: str,
            name: str,
            overwrite: bool = True
    ) -> Optional[int]:
        """
          Materialize dataframe as a table in a database.

        :param data: Dataframe to materialize as a table.
        :param schema: Schema of a table.
        :param name: Name of a table.
        :param overwrite: Whether to append or overwrite the data, including schema.
        :return:
        """

        try:
            if overwrite:
                self._get_connection().execute(f"DROP TABLE IF EXISTS {schema}.{name}")

            return data.to_sql(
                name=name,
                schema=schema,
                con=self._get_connection(),
                index=False,
                if_exists='append' if not overwrite else 'replace',
            )
        except SQLAlchemyError as ex:
            self._logger.error("Error while materializing a dataframe into {schema}.{table}", schema=schema, table=name, exception=ex)
            return None
