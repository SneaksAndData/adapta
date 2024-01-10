"""
 Models used by Hive storages.
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

import os
from dataclasses import dataclass
from typing import Optional

from adapta.storage.database.azure_sql import AzureSqlClient
from adapta.storage.models.base import DataPath, DataProtocols
from adapta.storage.database.models import DatabaseType
from adapta.logs import SemanticLogger


@dataclass
class HivePath(DataPath):
    """
    Virtual representation of a Hive entity path.
    """

    def base_uri(self) -> str:
        raise NotImplementedError

    @classmethod
    def from_uri(cls, url: str) -> "DataPath":
        raise NotImplementedError

    def to_uri(self) -> str:
        raise NotImplementedError

    hive_server: str
    hive_server_port: str
    hive_database: str
    hive_schema: str
    hive_engine: str
    path: str
    protocol: str = DataProtocols.HIVE.value
    database_type: DatabaseType = DatabaseType.SQL_SERVER_ODBC
    hive_table: Optional[str] = None

    @classmethod
    def from_hdfs_path(
        cls,
        hdfs_path: str,
        database_type: Optional[DatabaseType] = DatabaseType.SQL_SERVER_ODBC,
    ) -> "HivePath":
        # sample path
        # hive://engine@my-hive-server.net:1234/database/schema/table
        assert "@" in hdfs_path and hdfs_path.startswith(
            "hive://"
        ), "Invalid Hive path supplied. Please use the following format: hive://<engine>@<server address>:<server port>/database/schema/table"

        return HivePath(
            hive_server=hdfs_path.split("@")[1].split("/")[0].split(":")[0],
            hive_server_port=hdfs_path.split("@")[1].split("/")[0].split(":")[1],
            hive_database=hdfs_path.split("@")[1].split("/")[1],
            hive_schema=hdfs_path.split("@")[1].split("/")[2],
            hive_table=hdfs_path.split("@")[1].split("/")[3],
            hive_engine=hdfs_path.split("@")[0].split("//")[1],
            path="/".join(hdfs_path.split("@")[1].split("/")[1:]),
            database_type=database_type,
        )

    @staticmethod
    def from_hive_name(
        schema: str,
        table: str,
        database_type: Optional[DatabaseType] = DatabaseType.SQL_SERVER_ODBC,
    ) -> "HivePath":
        """
         Creates a HivePath from schema and table names. Relies on the rest of Hive connection info being provided through environment.

        :param schema: Hive table schema.
        :param table: Hive table name.
        :return: A valid HivePath
        """
        assert (
            os.getenv("PROTEUS__HIVE_SERVER")
            and os.getenv("PROTEUS__HIVE_SERVER_PORT")
            and os.getenv("PROTEUS__HIVE_SERVER_DATABASE")
            and os.getenv("PROTEUS__HIVE_SERVER_ENGINE")
        ), "PROTEUS__HIVE_SERVER, PROTEUS__HIVE_SERVER_PORT and PROTEUS__HIVE_SERVER_ENGINE must be set to construct a valid HivePath"

        return HivePath(
            hive_server=os.getenv("PROTEUS__HIVE_SERVER"),
            hive_server_port=os.getenv("PROTEUS__HIVE_SERVER_PORT"),
            hive_database=os.getenv("PROTEUS__HIVE_SERVER_DATABASE"),
            hive_engine=os.getenv("PROTEUS__HIVE_SERVER_ENGINE"),
            hive_schema=schema,
            hive_table=table,
            path=f"{os.getenv('PROTEUS__HIVE_SERVER_DATABASE')}/{schema}/{table}",
            database_type=database_type,
        )

    def _check_path(self):
        assert not self.path.startswith("/"), "Path should not start with /"

    def to_hdfs_path(self) -> str:
        self._check_path()
        return f"{self.protocol}://{self.hive_engine}@{self.hive_server}:{self.hive_server_port}/{self.path}"

    def get_physical_path(self, logger: SemanticLogger) -> Optional[str]:
        """
         Converts this virtual HivePath to a physical HDFS path.

        :param logger: CompositeLogger for SQL client.
        :return: Valid HDFS path for the hive entity.
        """
        if self.hive_engine == "sqlserver":
            with AzureSqlClient(
                logger=logger,
                host_name=self.hive_server,
                port=int(self.hive_server_port),
                database=self.hive_database,
                user_name=os.environ["PROTEUS__HIVE_USER"],
                password=os.environ["PROTEUS__HIVE_PASSWORD"],
                database_type=self.database_type,
            ) as hive_db_client:
                db_info = hive_db_client.query(f"select * from DBS where name = '{self.hive_schema}'").to_dict(
                    orient="records"
                )
                assert (
                    len(db_info) == 1
                ), "Hive query for DBS table returned more than 1 row. Hive Metastore database schema version must be >=2.*,<=3.*"
                db_id, db_location = db_info[0]["DB_ID"], db_info[0]["DB_LOCATION_URI"]

                tbl_info = hive_db_client.query(
                    f"select * from TBLS where db_id = {db_id} and TBL_NAME = '{self.hive_table}'"
                ).to_dict(orient="records")
                assert (
                    len(tbl_info) == 1
                ), "Hive query for TBLS table returned more than 1 row. Hive Metastore database schema version must be >=2.*,<=3.*"
                sd_id, tbl_type = tbl_info[0]["SD_ID"], tbl_info[0]["TBL_TYPE"]

                if tbl_type == "EXTERNAL_TABLE":
                    path = hive_db_client.query(
                        f"select * from SERDE_PARAMS where SERDE_ID = {sd_id} and PARAM_KEY = 'path'"
                    ).to_dict(orient="records")
                    assert (
                        len(path) == 1
                    ), "Hive query for SERDE_PARAMS table returned more than 1 row. Hive Metastore database schema version must be >=2.*,<=3.*"
                    return path[0]["PARAM_VALUE"]

                if tbl_type == "MANAGED_TABLE":
                    return f"{db_location}/{self.hive_table}"

                return None

        raise NotImplementedError(f"Engine {self.hive_engine} is not supported.")

    def to_delta_rs_path(self) -> str:
        raise NotImplementedError
