#  Copyright (c) 2023. ECCO Sneaks & Data
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

from adapta.storage.models.hive import HivePath


def test_from_hdfs_path():
    hive_path = HivePath.from_hdfs_path(
        "hive://sqlserver@servername.database.windows.net:1433/database/schema/table"
    )
    assert hive_path.hive_engine == "sqlserver"
    assert hive_path.hive_server == "servername.database.windows.net"
    assert hive_path.hive_server_port == "1433"
    assert hive_path.hive_database == "database"
    assert hive_path.hive_schema == "schema"
    assert hive_path.hive_table == "table"
