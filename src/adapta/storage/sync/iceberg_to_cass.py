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


def sync_iceberg_to_cassandra(
    iceberg_catalog: Catalog,
    client: CassandraClient,
    iceberg_source: DataSocket,
    version_field: str,
    cassandra_target: DataSocket,
    chunk_size: int,
    logger: LoggerInterface,
) -> None:
    """
    Synchronizes data from the provided Iceberg source to Cassandra target table. Assumes schemas are compatible.
    """
    source_path: IcebergPath = iceberg_source.parse_data_path()
    target_path: CassandraPath = cassandra_target.parse_data_path()
    cassandra_model = target_path.model_class()

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
        for batch in source_data.collect_batches(chunk_size=chunk_size, maintain_order=False):
            client.upsert_batch(
                batch.to_dicts(), cassandra_model, target_path.keyspace, target_path.table, batch_size=batch.height
            )
            total_synced_records += batch.height
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
            for batch in inserts.collect_batches(chunk_size=chunk_size, maintain_order=True):
                client.upsert_batch(
                    batch.to_dicts(), cassandra_model, target_path.keyspace, target_path.table, batch_size=batch.height
                )
                total_synced_records += batch.height
            if updates is not None:
                for batch in updates.collect_batches(chunk_size=chunk_size, maintain_order=True):
                    client.upsert_batch(
                        batch.to_dicts(),
                        cassandra_model,
                        target_path.keyspace,
                        target_path.table,
                        batch_size=batch.height,
                    )
                    total_synced_records += batch.height
            if deletes is not None:
                for batch in deletes.collect_batches(chunk_size=chunk_size, maintain_order=True):
                    for row in batch.to_dicts():
                        client.delete_entity_by_key(cassandra_model, row, target_path.table)
                        total_synced_records += batch.height
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
