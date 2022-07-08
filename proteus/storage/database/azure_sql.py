import time
from typing import Optional, List

import pandas
from sqlalchemy import text

from proteus.logs import ProteusLogger
from proteus.storage.database.odbc import OdbcClient
from proteus.storage.database.models import DatabaseType
from proteus.utils import doze


class AzureSqlClient(OdbcClient):
    def __init__(
            self,
            logger: ProteusLogger,
            database_type: DatabaseType,
            host_name: str,
            user_name: str
    ):
        super().__init__(logger, database_type, host_name, user_name)

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
