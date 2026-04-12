#  Copyright (c) 2023-2026. ECCO Data & AI and other project contributors.
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
import pytest

from adapta.storage.models.snowflake import SnowflakePath
from adapta.storage.models import parse_data_path
from adapta.process_communication import DataSocket

_TABLE_SELECT = 'SELECT * FROM "mydb"."myschema"."mytable"'
_HDFS_TABLE = f"snowflake://{_TABLE_SELECT}"
_BAD_PATH = "s3://wrong/protocol/entirely"


def test_from_hdfs_path_sql_body():
    path = SnowflakePath.from_hdfs_path('snowflake://SELECT a, b FROM "db"."sch"."t" JOIN other o ON t.id = o.id')
    assert path.query == 'SELECT a, b FROM "db"."sch"."t" JOIN other o ON t.id = o.id'


def test_from_hdfs_path_table_select_uri():
    path = SnowflakePath.from_hdfs_path(_HDFS_TABLE)
    assert path.query == _TABLE_SELECT


def test_from_hdfs_path_invalid():
    with pytest.raises(AssertionError):
        SnowflakePath.from_hdfs_path(_BAD_PATH)


def test_to_hdfs_path_not_implemented():
    path = SnowflakePath.from_hdfs_path(_HDFS_TABLE)
    with pytest.raises(NotImplementedError):
        path.to_hdfs_path()


def test_parse_data_path_returns_snowflake_path():
    result = parse_data_path(_HDFS_TABLE)
    assert isinstance(result, SnowflakePath)
    assert result.query == _TABLE_SELECT


def test_datasocket_parse_data_path_snowflake():
    socket = DataSocket(alias="test", data_path=_HDFS_TABLE, data_format="snowflake")
    result = socket.parse_data_path()
    assert isinstance(result, SnowflakePath)
    assert result.query == _TABLE_SELECT
