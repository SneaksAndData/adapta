from enum import Enum
from typing import final

from polars import LazyFrame
from pyiceberg.catalog import Catalog

from adapta.logs import LoggerInterface
from adapta.process_communication import DataSocket
from adapta.storage.distributed_object_store.v3.cassandra_client import CassandraClient, get_mapper
from adapta.storage.iceberg.v1 import (
    get_changes,
    get_current_snapshot,
    get_property,
    load_using_catalog,
    set_property,
)
from adapta.storage.models import CassandraPath, IcebergPath


@final
class CassandraUploadMode(Enum):
    CONCURRENT_BATCH = "batch"
    CONCURRENT_NATIVE = "native"


def sync_iceberg_to_cassandra(
    iceberg_catalog: Catalog,
    client: CassandraClient,
    iceberg_source: DataSocket,
    version_field: str,
    cassandra_target: DataSocket,
    read_chunk_size: int,
    logger: LoggerInterface,
    threads: int,
    upload_mode: CassandraUploadMode = CassandraUploadMode.CONCURRENT_BATCH,
) -> None:
    """
    Synchronizes data from the provided Iceberg source to Cassandra target table. Assumes schemas are compatible.

    When uploading a lot of rows, use exec profile with high requests/connection for Cassandra client.

    profile = ExecutionProfile(
        request_timeout=30.0,
        max_requests_per_connection=2048  # Allows thousands of individual async writes on one connection
    )

    """

    source_path: IcebergPath = iceberg_source.parse_data_path()
    target_path: CassandraPath = cassandra_target.parse_data_path()
    cassandra_model = target_path.model_class()

    def _sync_lazyframe(source: LazyFrame) -> int:
        if upload_mode == CassandraUploadMode.CONCURRENT_BATCH:
            return client.upload_concurrent_batch(
                source=source,
                table_name=target_path.table,
                entity_type=cassandra_model,
                keyspace=target_path.keyspace,
                batch_size=read_chunk_size,
                threads=threads,
            )
        if upload_mode == CassandraUploadMode.CONCURRENT_NATIVE:
            return client.upload_concurrent_native(
                source=source,
                table_name=target_path.table,
                entity_type=cassandra_model,
                keyspace=target_path.keyspace,
                batch_size=read_chunk_size,
                threads=threads,
            )
        raise ValueError(f"Unsupported upload mode: {upload_mode}")

    def _sync_deletes(source: LazyFrame) -> int:
        return client.delete_batch_concurrent(
            source=source,
            table_name=target_path.table,
            entity_type=cassandra_model,
            keyspace=target_path.keyspace,
            batch_size=read_chunk_size,
            threads=threads,
        )

    logger.info(
        "Running incremental sync to table {sync_to_table}. Looking for changes to sync.",
        sync_to_table=target_path.table,
    )
    last_synced_snapshot = get_property(
        source_path.schema, source_path.table, iceberg_catalog, "adapta.cassandra.last-synced-snapshot-id"
    )
    mapper = get_mapper(
        data_model=cassandra_model,
        table_name=iceberg_source.alias,
        keyspace="any",
    )
    total_synced_records = 0
    if not last_synced_snapshot or last_synced_snapshot == "-1":
        logger.info("Last sync snapshot data not available. Will perform a full sync.")
        current_snapshot = get_current_snapshot(source_path.schema, source_path.table, iceberg_catalog)
        source_data: LazyFrame = load_using_catalog(
            source_path.schema,
            source_path.table,
            iceberg_catalog,
            lazy_read=True,
        ).to_polars()
        total_synced_records = _sync_lazyframe(source_data)
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
            inserts, updates, deletes = get_changes(
                source_path.schema,
                source_path.table,
                iceberg_catalog,
                int(last_synced_snapshot),
                current_snapshot,
                tracking_column=version_field,
                primary_key_columns=tuple(mapper.primary_keys),
            )
            # insert/update first
            total_synced_records += _sync_lazyframe(inserts)
            if updates is not None:
                total_synced_records += _sync_lazyframe(updates)
            # keep deletes out of parallelism for now
            if deletes is not None:
                total_synced_records += _sync_deletes(deletes)
            set_property(
                source_path.schema,
                source_path.table,
                iceberg_catalog,
                "adapta.cassandra.last-synced-snapshot-id",
                str(current_snapshot),
            )

    logger.info(
        "Table {target} has been successfully synced with {source}, records changed: {records}",
        target=target_path.table,
        source=source_path.table,
        records=total_synced_records,
    )
