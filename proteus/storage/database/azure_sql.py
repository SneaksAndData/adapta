"""
 ODBC client extension for Azure SQL.
"""

from typing import Optional

import pandas
from sqlalchemy import text

from proteus.logs import ProteusLogger
from proteus.storage.database.odbc import OdbcClient
from proteus.storage.database.models import DatabaseType
from proteus.utils import doze


class AzureSqlClient(OdbcClient):
    """
     Azure SQL (cloud) ODBC client.
    """
    def __init__(
            self,
            logger: ProteusLogger,
            host_name: str,
            user_name: str,
            password: str,
            database: Optional[str] = None,
            port: Optional[int] = 1433
    ):
        """
          Creates an instance of an Azure SQL ODBC client.

        :param logger: Logger instance for database operations.
        :param host_name: Hostname of the instance.
        :param user_name: User to connect with
        :param password: SQL user password to use with this instance.
        :param database: Database to connect to.
        :param port: Connection port. Defaults to 1433.
        """
        super().__init__(
            logger=logger,
            database_type=DatabaseType.SQL_SERVER_ODBC,
            host_name=host_name,
            user_name=user_name,
            database=database,
            password=password,
            port=port
        )

    def scale_instance(self, target_objective='HS_Gen4_8', timeout_seconds: Optional[int] = 180) -> Optional[bool]:
        """
          Scales up/down the connected database.

        :param target_objective: Target Azure SQL instance size.
        :param timeout_seconds: If provided, waits for the operation to complete within a specified interval. If a scale
          operation doesn't complete, function will return None.
        :return: Result of a scale operation.
        """
        def get_current_objective(client: OdbcClient) -> pandas.DataFrame:
            return client.query(
                'SELECT service_objective FROM sys.database_service_objectives'
            ).to_dict().get('service_objective', None)

        current_objective = get_current_objective(self)

        if current_objective == target_objective:
            return True

        _ = self._get_connection().execute(
            text(f"ALTER DATABASE [{self._database}] MODIFY (service_objective = '{target_objective}');")
        )

        if timeout_seconds:
            elapsed = 0
            while current_objective != target_objective and timeout_seconds > elapsed:
                elapsed += (doze(60) // 1e9)
                with self.fork() as client_fork:
                    current_objective = get_current_objective(client_fork)

            return current_objective == target_objective

        return True
