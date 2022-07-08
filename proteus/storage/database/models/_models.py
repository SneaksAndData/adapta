from dataclasses import dataclass
from enum import Enum
from typing import Dict


@dataclass
class SqlAlchemyDialect:
    dialect: str
    driver: Dict[str, str]


class DatabaseType(Enum):
    SQL_SERVER_ODBC = SqlAlchemyDialect(
        dialect='mssql+pyodbc',
        driver={
            "driver": "ODBC Driver 17 for SQL Server",
            "LongAsMax": "Yes",
        })
