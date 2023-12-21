"""
  Snowflake Client Wrapper
"""
from types import TracebackType
from typing import Optional

from pandas import DataFrame
import snowflake.connector

from snowflake.connector.errors import DatabaseError, ProgrammingError

from adapta.logs.models import LogLevel
from adapta.logs import SemanticLogger


class SnowflakeClient:
    """
    A wrapper around the Snowflake Python connector that provides a context manager for handling connections
    and transactions. It also includes a method for executing queries and returning the result as a Pandas DataFrame.

    :param user: The username for the Snowflake account.
    :param account: The account name for the Snowflake account.
    :param warehouse: The warehouse name for the Snowflake account.
    :param authenticator: The authentication mechanism to use for the Snowflake account.
    :param logger: The logger to use for logging messages. Defaults to a new SemanticLogger instance.
    """

    def __init__(
        self,
        user: str,
        account: str,
        warehouse: str,
        authenticator: str = "externalbrowser",
        logger: SemanticLogger = SemanticLogger().add_log_source(
            log_source_name="adapta-snowflake-client",
            min_log_level=LogLevel.INFO,
            is_default=True,
        ),
    ):
        self._user = user
        self._account = account
        self._warehouse = warehouse
        self._authenticator = authenticator
        self._logger = logger
        self._conn = None

    def __enter__(self) -> Optional["SnowflakeClient"]:
        """
        Enters the context manager and establishes a connection to the Snowflake database.
        :return: The SnowflakeClient instance, or None if there was an error connecting to the database.
        """
        try:
            self._conn = snowflake.connector.connect(
                user=self._user, account=self._account, warehouse=self._warehouse, authenticator=self._authenticator
            )
            return self
        except DatabaseError as ex:
            self._logger.error(
                "Error connecting to {account} for {user}", account=self._account, user=self._user, exception=ex
            )
            return None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """
        Exits the context manager and closes the database connection.

        :param exc_type: The type of the exception that was raised, if any.
        :param exc_val: The value of the exception that was raised, if any.
        :param exc_tb: The traceback of the exception that was raised, if any.
        """
        self._conn.close()
        if exc_val is not None:
            self._logger.error(f"An error occurred while closing the database connection: {exc_val}")

    def query(self, query: str) -> DataFrame | None:
        """
        Executes the given SQL query and returns the result as a Pandas DataFrame.

        :param query: The SQL query to execute.
        :return: An iterator of Pandas DataFrames, one for each result set returned by the query, or None if there was
            an error executing the query.
        """
        try:
            with self._conn.cursor() as cursor:
                return cursor.execute(query).fetch_pandas_all()
        except ProgrammingError as ex:
            self._logger.error("Error executing query {query}", query=query, exception=ex)
            return None
