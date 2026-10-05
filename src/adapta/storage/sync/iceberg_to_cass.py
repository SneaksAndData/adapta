import uuid

from polars import LazyFrame
from pyiceberg.catalog import Catalog

from adapta.logs import LoggerInterface
from adapta.metrics import MetricsProvider
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
from adapta.utils import run_time_metrics
from adapta.utils.concurrent_task_runner import ConcurrentTaskRunner, Executable


def sync_iceberg_to_cassandra(
    iceberg_catalog: Catalog,
    client: CassandraClient,
    iceberg_source: DataSocket,
    version_field: str,
    cassandra_target: DataSocket,
    read_chunk_size: int,
    logger: LoggerInterface,
    metrics: MetricsProvider,
    threads: int,
) -> None:
    """
    Synchronizes data from the provided Iceberg source to Cassandra target table. Assumes schemas are compatible.
    """

    def _fix_map_type(entity: dict) -> dict:
        """Polars remaps map<k, v> to list[{"key": ..., "value": ...}]"""
        fixed = {}
        for cell_key, cell_value in entity.items():
            if (
                isinstance(cell_value, list)
                and len(cell_value) > 0
                and "key" in cell_value[0]
                and "value" in cell_value[0]
            ):
                fixed[cell_key] = {a["key"]: a["value"] for a in cell_value}
            else:
                fixed[cell_key] = cell_value
        return fixed

    def _sync_lazyframe(source: LazyFrame) -> int:
        @run_time_metrics(metric_name="adapta.cassandra.iceberg_batch_upsert_duration")
        def _upsert_with_metric(entities: list[dict], **kwargs) -> int:
            client.upsert_batch(entities=entities, **kwargs)
            kwargs["logger"].info("Upserted {rows} rows", rows=len(entities))
            return len(entities)

        ctr = ConcurrentTaskRunner[int](
            [
                Executable(
                    _upsert_with_metric,
                    str(uuid.uuid4()),
                    kwargs={
                        "entities": [_fix_map_type(batch_row) for batch_row in source_batch.to_dicts()],
                        "entity_type": cassandra_model,
                        "keyspace": target_path.keyspace,
                        "table_name": target_path.table,
                        "batch_size": source_batch.height,
                        "metrics_provider": metrics,
                        "logger": logger,
                    },
                )
                for source_batch in source.collect_batches(chunk_size=read_chunk_size, maintain_order=False)
            ],
            num_threads=threads,
        )
        return sum([rows_synced for _, rows_synced in ctr.eager().items()])

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
                for batch in deletes.collect_batches(chunk_size=read_chunk_size, maintain_order=True):
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
