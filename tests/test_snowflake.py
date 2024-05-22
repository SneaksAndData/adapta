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
import pathlib
from unittest.mock import patch, MagicMock

import pytest
from deltalake import DeltaTable

from adapta.storage.database.snowflake_sql import SnowflakeClient

from adapta.storage.models.azure import AdlsGen2Path


@patch("adapta.storage.database.snowflake_sql.SnowflakeClient.query")
def test_publish_external_delta_table(
    mock_query: MagicMock,
):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/delta_table"
    snowflake_client = SnowflakeClient(user="", account="", warehouse="")
    path = AdlsGen2Path.from_hdfs_path("abfss://container@account.dfs.core.windows.net/test_schema/test_table")
    delta_table = DeltaTable(
        f"{test_data_path}",
    )
    snowflake_client.publish_external_delta_table(
        database="test_database",
        schema="test_schema",
        table="test_table",
        path=path,
        table_schema=delta_table.schema().to_pyarrow(),
    )

    mock_query.assert_any_call("create schema if not exists test_database.test_schema")
    mock_query.assert_any_call(
        """create stage if not exists test_database.test_schema.stage_test_table 
                storage_integration = account 
                url = azure://account.blob.core.windows.net/container/test_schema/test_table;"""
    )
    mock_query.assert_any_call(
        """
                create or replace external table "test_database"."test_schema"."test_table"
                (
                    "A" TEXT AS ($1:"A"::TEXT),
"B" TEXT AS ($1:"B"::TEXT)
                )
                
                location=test_database.test_schema.stage_test_table  
                auto_refresh = false   
                refresh_on_create=false   
                file_format = (type = parquet)    
                table_format = delta;"""
    )
    mock_query.assert_any_call('alter external table "test_database"."test_schema"."test_table" refresh;')


@patch("adapta.storage.database.snowflake_sql.SnowflakeClient.query")
def test_publish_external_delta_table_partitioned(
    mock_query: MagicMock,
):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/delta_table_with_partitions"
    snowflake_client = SnowflakeClient(user="", account="", warehouse="")
    path = AdlsGen2Path.from_hdfs_path("abfss://container@account.dfs.core.windows.net/test_schema/test_table")
    delta_table = DeltaTable(
        f"{test_data_path}",
    )
    snowflake_client.publish_external_delta_table(
        database="test_database",
        schema="test_schema",
        table="test_table",
        path=path,
        table_schema=delta_table.schema().to_pyarrow(),
        partition_columns=["colP"],
    )

    mock_query.assert_any_call("create schema if not exists test_database.test_schema")
    mock_query.assert_any_call(
        """create stage if not exists test_database.test_schema.stage_test_table 
                storage_integration = account 
                url = azure://account.blob.core.windows.net/container/test_schema/test_table;"""
    )
    mock_query.assert_any_call(
        """
                create or replace external table "test_database"."test_schema"."test_table"
                (
                    "colA" INTEGER AS ($1:"colA"::INTEGER),
"colB" TEXT AS ($1:"colB"::TEXT),
"colP" TEXT AS (split_part(split_part(metadata$filename, \'=\', 2), \'/\', 1))
                )
                partition by (colP)
                location=test_database.test_schema.stage_test_table  
                auto_refresh = false   
                refresh_on_create=false   
                file_format = (type = parquet)    
                table_format = delta;"""
    )
    mock_query.assert_any_call('alter external table "test_database"."test_schema"."test_table" refresh;')


@patch("adapta.storage.database.snowflake_sql.SnowflakeClient.query")
def test_publish_external_delta_table_skip_initialize(
    mock_query: MagicMock,
):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/delta_table"
    snowflake_client = SnowflakeClient(user="", account="", warehouse="")
    path = AdlsGen2Path.from_hdfs_path("abfss://container@account.dfs.core.windows.net/test_schema/test_table")
    delta_table = DeltaTable(
        f"{test_data_path}",
    )
    snowflake_client.publish_external_delta_table(
        database="test_database",
        schema="test_schema",
        table="test_table",
        path=path,
        skip_initialize=True,
        table_schema=delta_table.schema().to_pyarrow(),
    )

    with pytest.raises(AssertionError):
        mock_query.assert_any_call("create schema if not exists test_database.test_schema")

    mock_query.assert_any_call('alter external table "test_database"."test_schema"."test_table" refresh;')
