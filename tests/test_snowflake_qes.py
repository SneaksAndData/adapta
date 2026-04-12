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


def test_build_query_columns_wraps_inner_select():
    path = SnowflakePath.from_hdfs_path('snowflake://SELECT * FROM "db"."schema"."tbl"')
    query = SnowflakeQueryEnabledStore._build_query(
        inner_query=path.query,
        filter_expression=None,
        columns=["col1", "col2"],
        limit=None,
    )
    assert query == 'SELECT col1, col2 FROM (SELECT * FROM "db"."schema"."tbl")'


def test_build_query_pass_through_when_no_projection_filter_limit():
    inner = 'SELECT * FROM "db"."schema"."tbl"'
    query = SnowflakeQueryEnabledStore._build_query(
        inner_query=inner,
        filter_expression=None,
        columns=[],
        limit=None,
    )
    assert query == inner


def test_build_query_join_inner_wrapped_with_limit():
    inner = 'SELECT a.x FROM "d"."s"."t1" a JOIN "d"."s"."t2" b ON a.id = b.id'
    query = SnowflakeQueryEnabledStore._build_query(
        inner_query=inner,
        filter_expression=None,
        columns=[],
        limit=100,
    )
    assert query == f"SELECT * FROM ({inner}) LIMIT 100"


def test_from_connection_string_snowflake_parses_payload():
    connection_string = (
        "qes://engine=SNOWFLAKE;"
        'plaintext_credentials={"user":"alice","password":"secret"};'
        'settings={"account":"xy12345","warehouse":"COMPUTE_WH","role":"ANALYST"}'
    )

    store = SnowflakeQueryEnabledStore._from_connection_string(connection_string, lazy_init=True)

    assert isinstance(store, SnowflakeQueryEnabledStore)
    assert store.credentials.user == "alice"
    assert store.credentials.password == "secret"
    assert store.settings.account == "xy12345"
    assert store.settings.warehouse == "COMPUTE_WH"
    assert store.settings.role == "ANALYST"
