"""
  Snowflake Client Wrapper
"""

import os
from types import TracebackType
from typing import List, Optional

from pandas import DataFrame
import snowflake.connector
import pyarrow

from snowflake.connector.errors import DatabaseError, ProgrammingError

from adapta.logs.models import LogLevel
from adapta.logs import SemanticLogger

from adapta.storage.models.azure import AdlsGen2Path


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
        exc_type: Optional[type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional[TracebackType] = None,
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

    def query(self, query: str) -> Optional[DataFrame]:
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

    def _get_snowflake_type(self, data_type: pyarrow.DataType) -> str:
        """Maps pyarrow type to Snowflake type"""

        type_map = {
            pyarrow.types.is_string: "TEXT",
            pyarrow.types.is_integer: "INTEGER",
            pyarrow.types.is_floating: "FLOAT",
            pyarrow.types.is_timestamp: "TIMESTAMP_NTZ",
            pyarrow.types.is_date: "DATE",
            pyarrow.types.is_struct: "VARIANT",
            pyarrow.types.is_map: "VARIANT",
            pyarrow.types.is_list: "VARIANT",
            pyarrow.types.is_boolean: "BOOLEAN",
            pyarrow.types.is_binary: "BINARY",
        }

        for type_checker, snowflake_type_name in type_map.items():
            if type_checker(data_type):
                return snowflake_type_name

        if pyarrow.types.is_decimal(data_type):
            return f"DECIMAL({data_type.precision},{data_type.scale})"

        raise ValueError(f"found type:{data_type} which is currently not supported")

    def publish_external_delta_table(
        self,
        database: str,
        schema: str,
        table: str,
        path: AdlsGen2Path,
        table_schema: pyarrow.Schema,
        partition_columns: Optional[List[str]] = None,
        storage_integration: Optional[str] = None,
    ) -> None:
        """
        Creates delta table as external table in Snowflake

        :param database: name of the database, in Snowflake, to create the table
        :param schema: name of the schema, in Snowflake, to create the table
        :param table: name of the table to be created in Snowflake
        :param path: path to the delta table in datalake
        :param storage_integration: name of the storage integration to use in Snowflake. Default to the name of the storage account
        """

        self.query(f"create schema if not exists {database}.{schema}")

        self.query(
            f"""create stage if not exists {database}.{schema}.stage_{table} 
            storage_integration = {storage_integration if storage_integration is not None else path.account} 
            url = azure://{path.account}.blob.core.windows.net/{path.container}/{path.path};"""
        )

        if partition_columns is not None:
            partition_expr = ",".join(partition_columns)
            partition_select = [
                f"\"{partition_column}\" TEXT AS (split_part(split_part(metadata$filename, '=', {2 + i}), '/', 1))"
                for i, partition_column in enumerate(partition_columns)
            ]
        else:
            partition_expr = ""
            partition_select = []
            partition_columns = []

        snowflake_columns = [
            (column.name, self._get_snowflake_type(column.type))
            for column in table_schema
            if column.name not in partition_columns
        ]

        columns = [
            f'"{column}" {col_type} AS ($1:"{column}"::{col_type})' for column, col_type in snowflake_columns
        ] + partition_select

        column_expr = ("," + os.linesep).join(columns)

        self.query(
            f"""
            create or replace external table "{database}"."{schema}"."{table}"
            (
                {column_expr}
            )
            {f"partition by ({partition_expr})" if partition_expr else ""}
            location={database}.{schema}.stage_{table}  
            auto_refresh = false   
            refresh_on_create=false   
            file_format = (type = parquet)    
            table_format = delta;"""
        )

        self.query(f'alter external table "{database}"."{schema}"."{table}" refresh;')
