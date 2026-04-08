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


def test_from_hdfs_path():
    path = SnowflakePath.from_hdfs_path("snowflake://mydb/myschema/mytable")
    assert path.database == "mydb"
    assert path.schema == "myschema"
    assert path.table == "mytable"


@pytest.mark.parametrize(
    "bad_path",
    [
        "snowflake://only_two/parts",
        "snowflake://too/many/parts/here",
        "s3://wrong/protocol/entirely",
        "snowflake://",
    ],
)
def test_from_hdfs_path_invalid(bad_path):
    with pytest.raises(AssertionError):
        SnowflakePath.from_hdfs_path(bad_path)


def test_to_hdfs_path():
    path = SnowflakePath("database", "schema", "table").to_hdfs_path()
    assert path == "snowflake://database/schema/table"


def test_fully_qualified_name():
    path = SnowflakePath.from_hdfs_path("snowflake://mydb/myschema/mytable")
    assert path.fully_qualified_name == '"mydb"."myschema"."mytable"'


def test_parse_data_path_returns_snowflake_path():
    result = parse_data_path("snowflake://mydb/myschema/mytable")
    assert isinstance(result, SnowflakePath)
    assert result.database == "mydb"
    assert result.schema == "myschema"
    assert result.table == "mytable"


def test_datasocket_parse_data_path_snowflake():
    socket = DataSocket(alias="test", data_path="snowflake://mydb/myschema/mytable", data_format="snowflake")
    result = socket.parse_data_path()
    assert isinstance(result, SnowflakePath)
    assert result.database == "mydb"
