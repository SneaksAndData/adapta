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
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/delta_table_type_test"
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
        table_schema={column.name: column.type.type for column in delta_table.schema().fields},
    )

    mock_query.assert_any_call(query="create schema if not exists test_database.test_schema", fetch_pandas=False)
    mock_query.assert_any_call(
        query="""create stage if not exists test_database.test_schema.stage_test_table 
                storage_integration = account 
                url = azure://account.blob.core.windows.net/container/test_schema/test_table;""",
        fetch_pandas=False,
    )
    mock_query.assert_any_call(
        query="""
                create or replace external table "test_database"."test_schema"."test_table"
                (
                    "integer_field" INTEGER AS ($1:"integer_field"::INTEGER),
"string_field" TEXT AS ($1:"string_field"::TEXT),
"boolean_field" BOOLEAN AS ($1:"boolean_field"::BOOLEAN),
"double_field" FLOAT AS ($1:"double_field"::FLOAT),
"binary_field" BINARY AS ($1:"binary_field"::BINARY),
"float_field" FLOAT AS ($1:"float_field"::FLOAT),
"date_field" DATE AS ($1:"date_field"::DATE),
"timestamp_field" TIMESTAMP_NTZ AS ($1:"timestamp_field"::TIMESTAMP_NTZ),
"decimal_field" DECIMAL(10,2) AS ($1:"decimal_field"::DECIMAL(10,2)),
"map_field" VARIANT AS ($1:"map_field"::VARIANT),
"array_field" VARIANT AS ($1:"array_field"::VARIANT)
                )
                
                location=test_database.test_schema.stage_test_table  
                auto_refresh = false   
                refresh_on_create=false   
                file_format = (type = parquet)    
                table_format = delta;""",
        fetch_pandas=False,
    )
    mock_query.assert_any_call(
        query='alter external table "test_database"."test_schema"."test_table" refresh;', fetch_pandas=False
    )


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
        table_schema={column.name: column.type.type for column in delta_table.schema().fields},
        partition_columns=["colP"],
    )

    mock_query.assert_any_call(query="create schema if not exists test_database.test_schema", fetch_pandas=False)
    mock_query.assert_any_call(
        query="""create stage if not exists test_database.test_schema.stage_test_table 
                storage_integration = account 
                url = azure://account.blob.core.windows.net/container/test_schema/test_table;""",
        fetch_pandas=False,
    )
    mock_query.assert_any_call(
        query="""
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
                table_format = delta;""",
        fetch_pandas=False,
    )
    mock_query.assert_any_call(
        query='alter external table "test_database"."test_schema"."test_table" refresh;', fetch_pandas=False
    )


@patch("adapta.storage.database.snowflake_sql.SnowflakeClient.query")
def test_publish_external_delta_table_skip_initialize(
    mock_query: MagicMock,
):
    snowflake_client = SnowflakeClient(user="", account="", warehouse="")

    snowflake_client.publish_external_delta_table(
        database="test_database", schema="test_schema", table="test_table", refresh_metadata_only=True
    )

    with pytest.raises(AssertionError):
        mock_query.assert_any_call(query="create schema if not exists test_database.test_schema", fetch_pandas=False)

    mock_query.assert_any_call(
        query='alter external table "test_database"."test_schema"."test_table" refresh;', fetch_pandas=False
    )
