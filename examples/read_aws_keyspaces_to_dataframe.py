"""
Example script demonstrating how to read an AWS Keyspaces table into a DataFrame
using AwsKeyspaceClient.
"""

from dataclasses import dataclass, field
import os
import pandas as pd
import polars as pl

from adapta.storage.distributed_object_store.v3.aws_keyspaces import AwsKeyspaceClient
from adapta.storage.models.expression_dsl.astra_filter_expression import CassandraFilterExpression
from adapta.storage.models.expression_dsl.filter_expression import FilterExpression, FilterField


# 1. Define a dataclass schema representing your table structure (optional, used with filter_entities)
@dataclass
class ExampleEntity:
    algorithm: str = field(metadata={"is_primary_key": True, "is_partition_key": False})
    id: str = field(metadata={"is_primary_key": True, "is_partition_key": False})
    payload_content: str


def main():
    # Configure your connection parameters or read from environment
    keyspace = "nexus"
    table_name = "payload_buffer"
    region = "eu-central-1"
    client_name = "adapta_test"

    # Context manager automatically handles connect() and disconnect()
    with AwsKeyspaceClient(keyspace=keyspace, client_name=client_name, region=region, profile_name="ecco-dev") as client:
        # --- Option A: Read using CQL query via get_entities_raw ---
        print("Reading via raw CQL query...")
        meta_frame_pd = client.get_entities_raw(f"SELECT * FROM {keyspace}.{table_name} LIMIT 100")
        meta_frame_pl = client.get_entities_raw(f"SELECT * FROM {keyspace}.{table_name} LIMIT 100")

        # Convert to pandas DataFrame or polars DataFrame
        df_pandas: pd.DataFrame = meta_frame_pd.to_pandas()
        print(f"Loaded {len(df_pandas)} rows into pandas DataFrame:")
        print(df_pandas.head())
        df_polars: pl.DataFrame = meta_frame_pl.to_polars()
        print(f"Loaded {len(df_polars)} rows into pandas DataFrame:")
        print(df_polars.head())

        # --- Option B: Read using filter_entities with a typed data model ---
        print("\nReading via filter_entities...")
        filtered_meta_frame = client.filter_entities(
            model_class=ExampleEntity,
            table_name=table_name,
            key_column_filter_values=(FilterField("id") == "ee5227a6-1471-46af-aa8f-2d51767ba9b1") & (FilterField("algorithm") == "omni-channel-solver"),
        )

        df_filtered = filtered_meta_frame.to_pandas()
        print(f"Loaded {len(df_filtered)} filtered rows:")
        print(df_filtered.head())


if __name__ == "__main__":
    main()
