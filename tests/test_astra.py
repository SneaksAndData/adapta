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
from typing import Optional

import pytest
from adapta.storage.models.astra import AstraPath


@pytest.mark.parametrize(
    "path, expected_keyspace, expected_table, expected_class",
    [
        ("astra://keyspacename1@tablename0", "keyspacename1", "tablename0", None),
        ("astra://my_keyspace@tablename1", "my_keyspace", "tablename1", None),
        (
            "astra+my_lib.my_package.MyModel://my_keyspace@tablename1",
            "my_keyspace",
            "tablename1",
            "my_lib.my_package.MyModel",
        ),
        ("astra1+my_lib.my_package.MyModel://my_keyspace@tablename1", None, None, None),
    ],
)
def test_from_hdfs_path_valid(path: str, expected_keyspace: str, expected_table: str, expected_class: Optional[str]):
    astra_path = None
    try:
        astra_path = AstraPath.from_hdfs_path(path)
    except AssertionError:
        pass

    if astra_path is None:
        assert (expected_keyspace and expected_table and expected_class) is None
    else:
        assert (
            astra_path.keyspace == expected_keyspace
            and astra_path.table == expected_table
            and astra_path.model_class_name == expected_class
        )


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
