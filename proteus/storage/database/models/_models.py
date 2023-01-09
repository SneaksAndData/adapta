"""
  Models for relational database clients.
"""
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
    SQLITE_ODBC = SqlAlchemyDialect(dialect="sqlite+pysqlite", driver={})
