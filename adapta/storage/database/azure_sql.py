"""
 ODBC client extension for Azure SQL.
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

from typing import Optional

from sqlalchemy import text

from adapta.logs import SemanticLogger
from adapta.storage.database.odbc import OdbcClient
from adapta.storage.database.models import DatabaseType
from adapta.utils import doze


class AzureSqlClient(OdbcClient):
    """
    Azure SQL (cloud) ODBC client.
    """

    def __init__(
        self,
        logger: SemanticLogger,
        host_name: str,
        user_name: str,
        password: str,
        database: Optional[str] = None,
        port: Optional[int] = 1433,
        database_type: Optional[DatabaseType] = DatabaseType.SQL_SERVER_ODBC,
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
            database_type=database_type,
            host_name=host_name,
            user_name=user_name,
            database=database,
            password=password,
            port=port,
        )

    @property
    def size(self) -> str:
        """
        Current size (Service Objective) of a database in Azure.
        """
        return get_current_objective(self)

    def scale_instance(self, target_objective="HS_Gen4_8", max_wait_time: Optional[int] = 180) -> bool:
        """
          Scales up/down the connected database.

        :param target_objective: Target Azure SQL instance size.
        :param max_wait_time: If provided, waits for the operation to complete within a specified interval in seconds.
          If a scale operation doesn't complete within the specified period, function will return False, otherwise True.
        :return: Result of a scale operation.
          NB: if a timeout is not specified, True is returned,
          thus a user should perform a self-check if a downstream operation requires a scaled database.
        """

        assert self._database, "Database name must be provided when constructing a client for this method to execute."

        current_objective = get_current_objective(self)

        if current_objective == target_objective:
            return True

        _ = self._get_connection().execute(
            text(f"ALTER DATABASE [{self._database}] MODIFY (service_objective = '{target_objective}');")
        )

        self._logger.info(
            "Requested scale-up for {host}/{database}",
            host=self._host,
            database=self._database,
        )

        if max_wait_time:
            elapsed = 0
            while current_objective != target_objective and max_wait_time > elapsed:
                elapsed += doze(60) // 1e9
                with self.fork() as client_fork:
                    current_objective = get_current_objective(client_fork)
                    self._logger.info(
                        "Waiting for the scale-up to complete, elapsed {elapsed}s",
                        elapsed=elapsed,
                    )

            self._logger.info(
                "Scale-up {result} after {elapsed}s",
                result="completed" if current_objective == target_objective else "failed",
                elapsed=elapsed,
            )

            return current_objective == target_objective

        self._logger.info("Timeout not specified - exiting without awaiting the operation result")

        return True


def get_current_objective(client: AzureSqlClient) -> str:
    """
     Reads current database size for the specified client.

    :param client: Azure SQL database (ODBC) client.
    :return: Name of an active Azure SQL Service Objective.
    """
    return (
        client.query("SELECT service_objective FROM sys.database_service_objectives")
        .to_dict()
        .get("service_objective", None)
    )
