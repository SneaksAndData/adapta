from abc import ABC
from typing import Optional, Union, Iterator

import pandas
import sqlalchemy
from sqlalchemy.connectors import pyodbc
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError

from logs import ProteusLogger
from storage.database.models import ConnectionDialect, resolve_driver


class OdbcClient(ABC):
    def __init__(self, logger: ProteusLogger, dialect: ConnectionDialect, host_name: str, user_name: str,
                 database: Optional[str] = None,
                 password: Optional[str] = None, port: Optional[int] = None):
        self._dialect = dialect.value
        self._driver = resolve_driver(dialect)
        self._host = host_name
        self._database = database
        self._user = user_name
        self._password = password
        self._port = port
        self._logger = logger
        pyodbc.pooling = False

    def __enter__(self):
        connection_url: sqlalchemy.engine.URL = URL.create(
            drivername=self._dialect,
            host=self._host,
            database=self._database,
            username=self._user,
            password=self._password,
            port=self._port,
            query=self._driver
        )
        self._logger.info(
            'Connecting to {host}:{port} using dialect {dialect} and driver {driver}',
            host=self._host,
            port=self._port,
            dialect=self._dialect,
            driver=self._driver
        )
        try:
            self._engine = sqlalchemy.create_engine(connection_url, pool_pre_ping=True)
            self._connection = self._engine.connect()
        except SQLAlchemyError as ex:
            self._logger.error(
                'Error connecting to {host}:{port} using dialect {dialect} and driver {driver}',
                host=self._host,
                port=self._port,
                dialect=self._dialect,
                driver=self._driver,
                exception=ex
            )

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._connection.close()
        self._engine.dispose()

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
        if self._connection is None:
            self._logger.info(f'No connection is active. Please create one using with OdbcClient(..) as client: ...')
            return None
        if chunksize:
            return pandas.read_sql(query, con=self._connection, chunksize=chunksize)

        return pandas.read_sql(query, con=self._connection)

    # def materialize_query(
    #         self,
    #         : pd.DataFrame,
    #         schema: str,
    #         name: str,
    #         if_exists: str
    # ) -> None:
    #     """Write dataframe to database.
    #
    #     Args:
    #         df: The dataframe to write.
    #         schema: The schema in which the table is.
    #         name: The name of the table.
    #         if_exists: The action to take if the table already exists. Options: "append", "replace" and "truncate".
    #         "replace" removes the table and inserts it again with new data which in some cases may change datatypes.
    #         "truncate" only empties the table before insertion of new data.
    #     """
    #     if self.connection is None:
    #         missing_connection_error()
    #
    #     if if_exists == 'truncate':
    #         if self.connection.dialect.name == "sqlite":
    #             self.connection.execute(f'DELETE FROM {schema}.{name}')
    #         else:
    #             self.connection.execute(f'TRUNCATE TABLE {schema}.{name}')
    #         if_exists = 'append'
    #
    #     df.to_sql(
    #         name=name,
    #         schema=schema,
    #         con=self.connection,
    #         index=False,
    #         if_exists=if_exists,
    #     )
