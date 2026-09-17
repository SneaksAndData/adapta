import datetime
import uuid
import zlib
from dataclasses import dataclass, field
from datetime import UTC
from typing import Any

from adapta.storage.sync._models import SchemaOutOfSyncError
from polars import LazyFrame
from pyiceberg.catalog import Catalog

from adapta.logs import LoggerInterface
from adapta.process_communication import DataSocket
from adapta.storage.distributed_object_store.v3.cassandra_client import CassandraClient
from adapta.storage.iceberg.v1 import (
    get_changes,
    get_current_snapshot,
    get_property,
    get_schema,
    load_using_catalog,
    set_property,
)
from adapta.storage.models import CassandraPath, IcebergPath


@dataclass
class TableReference:
    """
    Pointers to actual tables based on key. This is used to allow graceful handover from old schema to new schema.
    """

    table_key: str = field(metadata={"is_primary_key": True})
    table_name: str
    supported_algorithm_version: str = field(metadata={"is_primary_key": True})
    reference_version: int

# sku sku_d3i21_312 1.2 1878993203 <-- current prod
# sku sku_d3i21_313 1.2-dev 1878993213
# sku sku_d3i21_314 1.3 1878993214
#

def sync_iceberg_to_cassandra(
    iceberg_catalog: Catalog,
    client: CassandraClient,
    iceberg_source: DataSocket,
    cassandra_model: Any,
    cassandra_target: DataSocket,
    chunk_size: int,
    logger: LoggerInterface,
) -> None:
    """
     Synchronizes data from the provided Iceberg source to Cassandra target table. Assumes schemas are compatible.
    """
    # perform schema check first
    # schema is recorded in iceberg metadata to avoid complex conversion between Cassandra and Iceberg types
    source_path: IcebergPath = iceberg_source.parse_data_path()
    target_path: CassandraPath = cassandra_target.parse_data_path()

    logger.info(
        "Running incremental sync to table {sync_to_table}. Looking for changes to sync.",
        sync_to_table=target_path.table,
    )
    last_synced_snapshot = get_property(
        source_path.schema, source_path.table, iceberg_catalog, "adapta.cassandra.last-synced-snapshot-id"
    )
    if not last_synced_snapshot or last_synced_snapshot == "-1":
        logger.info("Last sync snapshot data not available. Will perform a full sync.")
        current_snapshot = get_current_snapshot(source_path.schema, source_path.table, iceberg_catalog)
        source_data: LazyFrame = load_using_catalog(
            source_path.schema,
            source_path.table,
            iceberg_catalog,
            lazy_read=True,
        ).to_polars()
        for batch in source_data.collect_batches(chunk_size=chunk_size, maintain_order=False):
            client.upsert_batch(
                batch.to_dicts(), cassandra_model, target_path.keyspace, target_path.table, batch_size=batch.height
            )
        set_property(
            source_path.schema,
            source_path.table,
            iceberg_catalog,
            "adapta.cassandra.last-synced-snapshot-id",
            str(current_snapshot),
        )
    else:
        current_snapshot = get_current_snapshot(source_path.schema, source_path.table, iceberg_catalog)
        if int(last_synced_snapshot) != current_snapshot:
            logger.info(
                "Last sync snapshot: {snapshot}. Performing incremental sync to {latest_snapshot}",
                snapshot=last_synced_snapshot,
                latest_snapshot=current_snapshot,
            )
            changes: LazyFrame = get_changes(
                source_path.schema,
                source_path.table,
                iceberg_catalog,
                int(last_synced_snapshot),
                current_snapshot,
                lazy=True,
            ).to_polars()
            for batch in changes.collect_batches(chunk_size=chunk_size, maintain_order=True):
                client.upsert_batch(
                    batch.to_dicts(), cassandra_model, target_path.keyspace, target_path.table, batch_size=batch.height
                )
            set_property(
                source_path.schema,
                source_path.table,
                iceberg_catalog,
                "adapta.cassandra.last-synced-snapshot-id",
                str(current_snapshot),
            )

    logger.info(
        "Table {target} has been successfully synced with {source}",
        target=target_path.table,
        source=source_path.table,
    )
