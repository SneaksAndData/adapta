import datetime
import uuid
import zlib
from dataclasses import dataclass, field
from datetime import UTC
from typing import Any

from polars import LazyFrame
from pyiceberg.catalog import Catalog

from adapta.logs import LoggerInterface
from adapta.process_communication import DataSocket
from adapta.storage.distributed_object_store.v3.cassandra_client import CassandraClient
from adapta.storage.iceberg.v1 import get_changes, get_current_snapshot, get_property, get_schema, load_using_catalog
from adapta.storage.models import CassandraPath, IcebergPath


@dataclass
class TableReference:
    """
    Pointers to actual tables based on key. This is used to allow graceful handover from old schema to new schema.
    """

    table_key: str = field(metadata={"is_primary_key": True})
    table_name: str
    last_updated: int


def sync_with_handover(
    iceberg_catalog: Catalog,
    client: CassandraClient,
    iceberg_source: DataSocket,
    cassandra_model: Any,
    cassandra_target: DataSocket,
    chunk_size: int,
    logger: LoggerInterface,
) -> None:
    # perform schema check first
    # schema is recorded in iceberg metadata to avoid complex conversion between Cassandra and Iceberg types
    source_path: IcebergPath = iceberg_source.parse_data_path()
    recorded_schema_hash = get_property(
        source_path.schema, source_path.table, iceberg_catalog, "adapta.cassandra.schema-hash"
    )
    source_schema = get_schema(source_path.schema, source_path.table, iceberg_catalog)

    source_schema_hash = hex(zlib.crc32(source_schema.model_dump_json().encode("utf-8")))
    target_path: CassandraPath = cassandra_target.parse_data_path()

    def _run_sync(sync_to_table: str) -> None:
        logger.info(
            "Running incremental sync to table {sync_to_table}. Looking for changes to sync.",
            sync_to_table=sync_to_table,
        )
        last_synced_snapshot = get_property(
            source_path.schema, source_path.table, iceberg_catalog, "adapta.cassandra.last-synced-snapshot-id"
        )
        if not last_synced_snapshot or last_synced_snapshot == "-1":
            logger.info("Last sync snapshot data not available. Will perform a full sync.")
            source_data: LazyFrame = load_using_catalog(
                source_path.schema,
                source_path.table,
                iceberg_catalog,
                lazy_read=True,
            ).to_polars()
            for batch in source_data.collect_batches(chunk_size=chunk_size, maintain_order=False):
                client.upsert_batch(
                    batch.to_dicts(), cassandra_model, target_path.keyspace, target_path.table, batch_size=batch.count()
                )
        else:
            current_snapshot = get_current_snapshot(source_path.schema, source_path.table, iceberg_catalog)
            logger.info(
                "Last sync snapshot: {snapshot}. Performing incremental sync to {latest_snapshot}",
                snapshot=last_synced_snapshot,
                latest_snapshot=current_snapshot,
            )
            changes: LazyFrame = get_changes(
                source_path.schema,
                source_path.table,
                iceberg_catalog,
                last_synced_snapshot,
                current_snapshot,
                lazy=True,
            )
            for batch in changes.collect_batches(chunk_size=chunk_size, maintain_order=True):
                client.upsert_batch(
                    batch.to_dicts(), cassandra_model, target_path.keyspace, target_path.table, batch_size=batch.count()
                )

        logger.info(
            "Table {target} has been successfully synced with {source}",
            target=target_path.table,
            source=source_path.table,
        )

    if source_schema_hash != recorded_schema_hash:
        logger.info("Source schema has changed, will create a new table and run a handover")
        new_table_name = f"{source_path.table}_{str(uuid.uuid4()).replace('-', '_')}"
        client.create_table(cassandra_model, new_table_name, target_path.keyspace)
        _run_sync(new_table_name)
        client.upsert_entity(
            TableReference(
                table_key=source_path.table, table_name=new_table_name, last_updated=datetime.now(tz=UTC).timestamp()
            ),
            keyspace=target_path.keyspace,
            table_name="references",
        )
        return

    _run_sync(target_path.table)
