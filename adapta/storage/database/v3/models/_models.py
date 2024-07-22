"""
  Models for relational database clients.
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

from dataclasses import dataclass
from enum import Enum
from typing import Dict


@dataclass
class SqlAlchemyDialect:
    """
    Configuration for SqlAlchemy Engine Dialect
    """

    dialect: str
    driver: Dict[str, str]


class DatabaseType(Enum):
    """
    Pre-configured SqlAlchemy dialects for various clients.
    """

    SQL_SERVER_ODBC = SqlAlchemyDialect(
        dialect="mssql+pyodbc",
        driver={
            "driver": "ODBC Driver 17 for SQL Server",
            "LongAsMax": "Yes",
        },
    )
    SQL_SERVER_ODBC_V18 = SqlAlchemyDialect(
        dialect="mssql+pyodbc",
        driver={
            "driver": "ODBC Driver 18 for SQL Server",
            "LongAsMax": "Yes",
        },
    )
    SQLITE_ODBC = SqlAlchemyDialect(dialect="sqlite+pysqlite", driver={})
