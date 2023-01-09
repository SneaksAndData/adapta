import logging

import pytest

from proteus.logs.models import LogLevel
from proteus.logs import ProteusLogger
from proteus.storage.database.odbc import OdbcClient
from proteus.storage.database.models import DatabaseType


@pytest.fixture
def sqlite():
    proteus_logger = ProteusLogger().add_log_source(
        log_source_name="sqlite", min_log_level=LogLevel.INFO, is_default=True
    )
    return OdbcClient(
        logger=proteus_logger,
        database_type=DatabaseType.SQLITE_ODBC,
    )


@pytest.fixture
def restore_logger_class():
    _class = logging.getLoggerClass()
    yield
    pass
    logging.setLoggerClass(_class)
