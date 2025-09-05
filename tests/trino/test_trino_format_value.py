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

from adapta.storage.models import TrinoPath


@pytest.mark.parametrize(
    "path, expected_query",
    [
        ("trino://select * from table", "select * from table"),
        ("trino:// select * from table2", " select * from table2"),
        ("trino1://lakehousename1@schemaname1@tablename1", None),
    ],
)
def test_from_hdfs_path_valid(path: str, expected_query: str):
    trino_path = None
    try:
        trino_path = TrinoPath.from_hdfs_path(path)
    except AssertionError:
        pass

    if trino_path is None:
        assert expected_query is None
    else:
        assert trino_path.query == expected_query


@pytest.mark.parametrize(
    "path",
    [
        "invalid_path",
        "//invalid_path",
        "://invalid_path",
        "trino1://select * from table",
        "Trino://select * from table",
    ],
)
def test_from_hdfs_path_invalid(path: str):
    with pytest.raises(AssertionError):
        TrinoPath.from_hdfs_path(path)
