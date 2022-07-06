from enum import Enum
from typing import Optional, Dict


class ConnectionDialect(Enum):
    SQL_SERVER_ODBC = 'mssql+pyodbc'


def resolve_driver(dialect: ConnectionDialect) -> Optional[Dict]:
    return {
        ConnectionDialect.SQL_SERVER_ODBC: {
            "driver": "ODBC Driver 17 for SQL Server",
            "LongAsMax": "Yes",
        }
    }.get(dialect, None)
