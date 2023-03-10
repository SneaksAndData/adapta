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

import logging

import pytest

from adapta.logs.models import LogLevel
from adapta.logs import CompositeLogger
from adapta.storage.database.odbc import OdbcClient
from adapta.storage.database.models import DatabaseType


@pytest.fixture
def sqlite():
    c_logger = CompositeLogger().add_log_source(
        log_source_name="sqlite", min_log_level=LogLevel.INFO, is_default=True
    )
    return OdbcClient(
        logger=c_logger,
        database_type=DatabaseType.SQLITE_ODBC,
    )


@pytest.fixture
def restore_logger_class():
    _class = logging.getLoggerClass()
    yield
    pass
    logging.setLoggerClass(_class)
