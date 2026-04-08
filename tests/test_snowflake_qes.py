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

from adapta.storage.models.snowflake import SnowflakePath
from adapta.storage.query_enabled_store._qes_snowflake import SnowflakeQueryEnabledStore


def test_build_query_no_filter_no_limit():
    path = SnowflakePath.from_hdfs_path("snowflake://db/schema/tbl")
    query = SnowflakeQueryEnabledStore._build_query(
        table_fqn=path.fully_qualified_name,
        filter_expression=None,
        columns=["col1", "col2"],
        limit=None,
    )
    assert query == 'SELECT col1, col2 FROM "db"."schema"."tbl"'
