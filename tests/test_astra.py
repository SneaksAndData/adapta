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

import pytest
from adapta.storage.models.astra import AstraPath


@pytest.mark.parametrize(
    "path, expected_keyspace, expected_table",
    [
        ("astra://keyspacename1@tablename0", "keyspacename1", "tablename0"),
        ("astra://my_keyspace@tablename1", "my_keyspace", "tablename1"),
    ],
)
def test_from_hdfs_path_valid(path: str, expected_keyspace: str, expected_table: str):
    astra_path = AstraPath.from_hdfs_path(path)
    assert astra_path.keyspace == expected_keyspace
    assert astra_path.table == expected_table


@pytest.mark.parametrize(
    "path",
    [
        "invalid_path",
        "//invalid_path",
        "://invalid_path",
    ],
)
def test_from_hdfs_path_invalid(path: str):
    with pytest.raises(AssertionError):
        AstraPath.from_hdfs_path(path)
