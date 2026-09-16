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

from adapta.storage.models.astra import AstraPath
from adapta.storage.models.cassandra import CassandraPath


@pytest.mark.parametrize(
    "path, path_type, expected_keyspace, expected_table, expected_class",
    [
        ("astra://keyspacename1@tablename0", AstraPath, "keyspacename1", "tablename0", None),
        ("astra://my_keyspace@tablename1", AstraPath, "my_keyspace", "tablename1", None),
        (
            "astra+my_lib.my_package.MyModel://my_keyspace@tablename1",
            AstraPath,
            "my_keyspace",
            "tablename1",
            "my_lib.my_package.MyModel",
        ),
        ("astra1+my_lib.my_package.MyModel://my_keyspace@tablename1", AstraPath, None, None, None),
        ("cass://keyspacename1@tablename0", CassandraPath, "keyspacename1", "tablename0", None),
        ("cass://my_keyspace@tablename1", CassandraPath, "my_keyspace", "tablename1", None),
        (
            "cass+my_lib.my_package.MyModel://my_keyspace@tablename1",
            CassandraPath,
            "my_keyspace",
            "tablename1",
            "my_lib.my_package.MyModel",
        ),
        ("cass1+my_lib.my_package.MyModel://my_keyspace@tablename1", CassandraPath, None, None, None),
    ],
)
def test_from_hdfs_path_valid(
    path: str, path_type: type[CassandraPath], expected_keyspace: str, expected_table: str, expected_class: str | None
):
    cass_path = None
    try:
        cass_path = path_type.from_hdfs_path(path)
    except AssertionError:
        pass

    if cass_path is None:
        assert (expected_keyspace and expected_table and expected_class) is None
    else:
        assert (
            cass_path.keyspace == expected_keyspace
            and cass_path.table == expected_table
            and cass_path.model_class_name == expected_class
        )


@pytest.mark.parametrize(
    "path, path_type",
    [
        ("invalid_path", AstraPath),
        ("//invalid_path", AstraPath),
        ("://invalid_path", AstraPath),
        ("invalid_path", CassandraPath),
        ("//invalid_path", CassandraPath),
        ("://invalid_path", CassandraPath),
    ],
)
def test_from_hdfs_path_invalid(path: str, path_type: type[CassandraPath]):
    with pytest.raises(AssertionError):
        path_type.from_hdfs_path(path)
