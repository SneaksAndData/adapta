"""
  Snowflake Client Wrapper
"""

import os
import re
from types import TracebackType
from typing import Self

import snowflake.connector

from snowflake.connector.errors import DatabaseError, ProgrammingError

from adapta.logs.models import LogLevel
from adapta.logs import SemanticLogger
from adapta.storage.models import S3Path, DataPath

from adapta.storage.models.azure import AdlsGen2Path
from adapta.utils.metaframe import MetaFrame


class SnowflakeClient:
    """
    A wrapper around the Snowflake Python connector that provides a context manager for handling connections
    and transactions. It also includes a method for executing queries and returning the result as a Pandas DataFrame.

    :param user: The username for the Snowflake account.
    :param account: The account name for the Snowflake account.
    :param warehouse: The warehouse name for the Snowflake account.
    :param authenticator: The authentication mechanism to use for the Snowflake account.
    :param logger: The logger to use for logging messages. Defaults to a new SemanticLogger instance.
    :param password: Optional - The password for the Snowflake user. Should be combined with `authenticator='snowflake'` to enable password authentication
    :param role: Optional - The role for the Snowflake user.
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
        password: str | None = None,
        role: str | None = None,
    ):
        self._user = user
        self._account = account
        self._warehouse = warehouse
        self._authenticator = "snowflake" if password else authenticator
        self._logger = logger
        self._password = password
        self._role = role
        self._conn = None

    def __enter__(self) -> Self | None:
        """
        Enters the context manager and establishes a connection to the Snowflake database.
        :return: The SnowflakeClient instance, or None if there was an error connecting to the database.
        """
        try:
            self._conn = snowflake.connector.connect(
                user=self._user,
                account=self._account,
                password=self._password,
                warehouse=self._warehouse,
                authenticator=self._authenticator,
                role=self._role,
            )
            return self
        except DatabaseError as ex:
            self._logger.error(
                "Error connecting to {account} for {user}", account=self._account, user=self._user, exception=ex
            )
            return None

    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
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

    def query(self, query: str, fetch_dataframe: bool = True) -> MetaFrame | None:
        """
        Executes the given SQL query and returns the result as a Pandas DataFrame.

        :param query: The SQL query to execute.
        :param fetch_dataframe: Fetch dataframes in batches, otherwise only execute the query
        :return: An iterator of Pandas DataFrames, one for each result set returned by the query, or None if there was
            an error executing the query.
        """
        try:
            with self._conn.cursor() as cursor:
                result = cursor.execute(query)
                if fetch_dataframe:
                    return MetaFrame.from_arrow(result.fetch_arrow_all(force_return_table=True))
                return None

        except ProgrammingError as ex:
            self._logger.error("Error executing query {query}", query=query, exception=ex)
            return None

    def _get_snowflake_type(self, data_type: str) -> str:
        """Maps delta type to Snowflake type"""

        type_map = {
            "string": "TEXT",
            "integer": "INTEGER",
            "float": "FLOAT",
            "double": "FLOAT",
            "timestamp": "TIMESTAMP_NTZ",
            "date": "DATE",
            "struct": "VARIANT",
            "map": "VARIANT",
            "array": "VARIANT",
            "boolean": "BOOLEAN",
            "binary": "BINARY",
        }

        snowflake_type = type_map.get(data_type, None)
        if snowflake_type:
            return snowflake_type

        if data_type.startswith("decimal"):
            decimal_info = [int(num) for num in re.findall(r"\d+", data_type)]
            return f"DECIMAL({decimal_info[0]},{decimal_info[1]})"

        raise ValueError(f"found type:{data_type} which is currently not supported")

    def publish_external_delta_table(
        self,
        database: str,
        schema: str,
        table: str,
        refresh_metadata_only: bool = False,
        path: DataPath | None = None,
        table_schema: dict[str, str] | None = None,
        partition_columns: list[str] | None = None,
        storage_integration: str | None = None,
    ) -> None:
        """
        Creates delta table as external table in Snowflake

        :param database: name of the database, in Snowflake, to create the table
        :param schema: name of the schema, in Snowflake, to create the table
        :param table: name of the table to be created in Snowflake
        :param refresh_metadata_only: Only refresh metadata, when table has already existed in snowflake.
                                      So skip the initializing phases like creating schema, creating external table, etc.
        :param path: path to the delta table in datalake
        :param table_schema: A mapping from column name to column type (the type should be in the lower case and supported by delta table)
                             , like {'ColumnA': 'struct', 'ColumnB': 'decimal(10, 2)'}
        :param partition_columns: A list of partition column names
        :param storage_integration: name of the storage integration to use in Snowflake. Default to the name of the storage account
        """

        def _get_azure_query(resolved_path: AdlsGen2Path):
            return (
                f"create stage if not exists {database}.{schema}.stage_{table}"
                + f" storage_integration = {storage_integration if storage_integration is not None else resolved_path.account}"
                + f" url = 'azure://{resolved_path.account}.blob.core.windows.net/{resolved_path.container}/{path.path}';"
            )

        def _get_s3_query(resolved_path: S3Path):
            return (
                f"create stage if not exists {database}.{schema}.stage_{table}"
                + f" endpoint = '{os.environ['S3_ENDPOINT__DNS_NAME']}'"
                + f" url = 's3compat://{resolved_path.bucket}/{resolved_path.path}'"
                + f" credentials = (AWS_KEY_ID = '{os.environ['S3_ENDPOINT__ACCESS_KEY_ID']}' AWS_SECRET_KEY = '{os.environ['S3_ENDPOINT__ACCESS_KEY']}');"
            )

        if not refresh_metadata_only:
            assert path, "Path to the delta table needed! Please check!"
            assert table_schema, "Table schema needed! Please check!"

            self.query(query=f"create schema if not exists {database}.{schema}", fetch_dataframe=False)

            if isinstance(path, AdlsGen2Path):
                query = _get_azure_query(path)
            elif isinstance(path, S3Path):
                query = _get_s3_query(path)
            else:
                raise ValueError(f"Path type {type(path)} is not supported!")

            self.query(
                query=query,
                fetch_dataframe=False,
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
                (column_name, self._get_snowflake_type(column_type))
                for column_name, column_type in table_schema.items()
                if column_name not in partition_columns
            ]

            columns = [
                f'"{column}" {col_type} AS ($1:"{column}"::{col_type})' for column, col_type in snowflake_columns
            ] + partition_select

            column_expr = ("," + os.linesep).join(columns)

            self.query(
                query=f"""
                create or replace external table "{database}"."{schema}"."{table}"
                (
                    {column_expr}
                )
                {f"partition by ({partition_expr})" if partition_expr else ""}
                location=@{database}.{schema}.stage_{table}  
                auto_refresh = false   
                refresh_on_create=false   
                file_format = (type = parquet)    
                table_format = delta;""",
                fetch_dataframe=False,
            )

        self.query(query=f'alter external table "{database}"."{schema}"."{table}" refresh;', fetch_dataframe=False)
