import dataclasses
from enum import Enum
from typing import Any, final

import polars
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
    write_using_catalog,
)
from adapta.storage.models import CassandraPath, IcebergPath


@final
class CassandraUploadMode(Enum):
    CONCURRENT_BATCH = "batch"
    CONCURRENT_NATIVE = "native"


def _get_custom_index_models(
    cassandra_model: type,
    source_table: str,
    target_table: str,
    version_field: str,
    logger: LoggerInterface,
) -> list[tuple[Any, str, str, str, list[str]]]:
    if not dataclasses.is_dataclass(cassandra_model):
        return []

    model_fields = {f.name: f for f in dataclasses.fields(cassandra_model)}
    custom_idx_fields = [f for f in dataclasses.fields(cassandra_model) if f.metadata.get("is_custom_index", False)]
    if not custom_idx_fields:
        return []

    orig_pks = [f for f in dataclasses.fields(cassandra_model) if f.metadata.get("is_primary_key", False)]
    ver_field = model_fields.get(version_field)

    index_models = []
    for idx_f in custom_idx_fields:
        idx_iceberg_table = f"{source_table}__idx_{idx_f.name}"
        idx_cassandra_table = f"{target_table}__idx_{idx_f.name}"

        fields_spec = [
            (
                idx_f.name,
                idx_f.type,
                dataclasses.field(metadata={"is_primary_key": True, "is_partition_key": True}),
            )
        ]
        for pk in orig_pks:
            if pk.name != idx_f.name:
                fields_spec.append((pk.name, pk.type, dataclasses.field()))
        if ver_field and ver_field.name != idx_f.name:
            fields_spec.append((ver_field.name, ver_field.type, dataclasses.field()))

        index_model = dataclasses.make_dataclass(f"{cassandra_model.__name__}__idx_{idx_f.name}", fields_spec)
        index_cols = [f[0] for f in fields_spec]
        logger.info(
            "Identified custom index field {field_name} with columns {columns}",
            field_name=idx_f.name,
            columns=index_cols,
        )
        index_models.append((index_model, idx_f.name, idx_iceberg_table, idx_cassandra_table, index_cols))

    return index_models


def _sync_custom_index_table(
    index_model: Any,
    idx_field_name: str,
    idx_iceberg_table: str,
    idx_cassandra_table: str,
    index_cols: list[str],
    source_schema: str,
    source_table: str,
    target_keyspace: str,
    iceberg_catalog: Catalog,
    client: CassandraClient,
    upload_mode: CassandraUploadMode,
    read_chunk_size: int,
    threads: int,
    logger: LoggerInterface,
    current_snapshot: int,
    is_full_sync: bool,
    source_data: LazyFrame | None = None,
    inserts: LazyFrame | None = None,
    updates: LazyFrame | None = None,
    deletes: LazyFrame | None = None,
) -> None:
    idx_last_synced = get_property(
        source_schema, idx_iceberg_table, iceberg_catalog, "adapta.cassandra.last-synced-snapshot-id"
    )
    if (
        is_full_sync
        or not idx_last_synced
        or idx_last_synced == "-1"
        or not iceberg_catalog.table_exists(identifier=(source_schema, idx_iceberg_table))
    ):
        logger.info(
            "Performing full sync for custom index table {iceberg_table} / {cassandra_table}",
            iceberg_table=idx_iceberg_table,
            cassandra_table=idx_cassandra_table,
        )
        full_data = (
            source_data
            if source_data is not None
            else load_using_catalog(
                source_schema,
                source_table,
                iceberg_catalog,
                version_id=current_snapshot,
                lazy_read=True,
            ).to_polars()
        )
        idx_data = full_data.select(index_cols)

        logger.info("Writing index data to Iceberg table {iceberg_table}", iceberg_table=idx_iceberg_table)
        write_using_catalog(
            schema_name=source_schema,
            table_name=idx_iceberg_table,
            catalog=iceberg_catalog,
            data=idx_data,
            overwrite=True,
        )

        logger.info("Creating Cassandra index table {cassandra_table}", cassandra_table=idx_cassandra_table)
        client.create_table(index_model, idx_cassandra_table, target_keyspace)

        logger.info("Syncing index table {cassandra_table} to Cassandra", cassandra_table=idx_cassandra_table)
        if upload_mode == CassandraUploadMode.CONCURRENT_BATCH:
            client.upload_concurrent_batch(
                source=idx_data,
                table_name=idx_cassandra_table,
                entity_type=index_model,
                keyspace=target_keyspace,
                batch_size=read_chunk_size,
                threads=threads,
            )
        elif upload_mode == CassandraUploadMode.CONCURRENT_NATIVE:
            client.upload_concurrent_native(
                source=idx_data,
                table_name=idx_cassandra_table,
                entity_type=index_model,
                keyspace=target_keyspace,
                batch_size=read_chunk_size,
                threads=threads,
            )
    else:
        logger.info(
            "Performing incremental sync for custom index table {iceberg_table} / {cassandra_table}",
            iceberg_table=idx_iceberg_table,
            cassandra_table=idx_cassandra_table,
        )
        idx_inserts = inserts.select(index_cols) if inserts is not None else None
        idx_updates = updates.select(index_cols) if updates is not None else None
        idx_deletes = deletes.select(index_cols) if deletes is not None else None

        idx_upsert_dfs = [df for df in [idx_inserts, idx_updates] if df is not None]
        if idx_upsert_dfs:
            idx_upserts = polars.concat(idx_upsert_dfs)
            logger.info(
                "Writing incremental updates to Iceberg index table {iceberg_table}",
                iceberg_table=idx_iceberg_table,
            )
            write_using_catalog(
                schema_name=source_schema,
                table_name=idx_iceberg_table,
                catalog=iceberg_catalog,
                data=idx_upserts,
                overwrite=False,
                merge_columns=[idx_field_name],
            )

        if idx_deletes is not None:
            del_rows = idx_deletes.collect().to_dicts()
            if del_rows:
                logger.info(
                    "Deleting {count} rows from Iceberg index table {iceberg_table}",
                    count=len(del_rows),
                    iceberg_table=idx_iceberg_table,
                )
                idx_iceberg_tbl = iceberg_catalog.load_table(identifier=(source_schema, idx_iceberg_table))
                for del_row in del_rows:
                    val = del_row[idx_field_name]
                    del_expr = f"{idx_field_name} = '{val}'" if isinstance(val, str) else f"{idx_field_name} = {val}"
                    idx_iceberg_tbl.delete(del_expr)

        # Sync changes to Cassandra
        if idx_inserts is not None:
            logger.info(
                "Syncing index inserts to Cassandra table {cassandra_table}", cassandra_table=idx_cassandra_table
            )
            if upload_mode == CassandraUploadMode.CONCURRENT_BATCH:
                client.upload_concurrent_batch(
                    source=idx_inserts,
                    table_name=idx_cassandra_table,
                    entity_type=index_model,
                    keyspace=target_keyspace,
                    batch_size=read_chunk_size,
                    threads=threads,
                )
            elif upload_mode == CassandraUploadMode.CONCURRENT_NATIVE:
                client.upload_concurrent_native(
                    source=idx_inserts,
                    table_name=idx_cassandra_table,
                    entity_type=index_model,
                    keyspace=target_keyspace,
                    batch_size=read_chunk_size,
                    threads=threads,
                )

        if idx_updates is not None:
            logger.info(
                "Syncing index updates to Cassandra table {cassandra_table}", cassandra_table=idx_cassandra_table
            )
            if upload_mode == CassandraUploadMode.CONCURRENT_BATCH:
                client.upload_concurrent_batch(
                    source=idx_updates,
                    table_name=idx_cassandra_table,
                    entity_type=index_model,
                    keyspace=target_keyspace,
                    batch_size=read_chunk_size,
                    threads=threads,
                )
            elif upload_mode == CassandraUploadMode.CONCURRENT_NATIVE:
                client.upload_concurrent_native(
                    source=idx_updates,
                    table_name=idx_cassandra_table,
                    entity_type=index_model,
                    keyspace=target_keyspace,
                    batch_size=read_chunk_size,
                    threads=threads,
                )

        if idx_deletes is not None:
            logger.info(
                "Syncing index deletes to Cassandra table {cassandra_table}", cassandra_table=idx_cassandra_table
            )
            client.delete_batch_concurrent(
                source=idx_deletes,
                table_name=idx_cassandra_table,
                entity_type=index_model,
                keyspace=target_keyspace,
                batch_size=read_chunk_size,
                threads=threads,
            )

    set_property(
        source_schema,
        idx_iceberg_table,
        iceberg_catalog,
        "adapta.cassandra.last-synced-snapshot-id",
        str(current_snapshot),
    )


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
    custom_index_models = _get_custom_index_models(
        cassandra_model=cassandra_model,
        source_table=source_path.table,
        target_table=target_path.table,
        version_field=version_field,
        logger=logger,
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
        for idx_meta in custom_index_models:
            _sync_custom_index_table(
                *idx_meta,
                source_schema=source_path.schema,
                source_table=source_path.table,
                target_keyspace=target_path.keyspace,
                iceberg_catalog=iceberg_catalog,
                client=client,
                upload_mode=upload_mode,
                read_chunk_size=read_chunk_size,
                threads=threads,
                logger=logger,
                current_snapshot=current_snapshot,
                is_full_sync=True,
                source_data=source_data,
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
            for idx_meta in custom_index_models:
                _sync_custom_index_table(
                    *idx_meta,
                    source_schema=source_path.schema,
                    source_table=source_path.table,
                    target_keyspace=target_path.keyspace,
                    iceberg_catalog=iceberg_catalog,
                    client=client,
                    upload_mode=upload_mode,
                    read_chunk_size=read_chunk_size,
                    threads=threads,
                    logger=logger,
                    current_snapshot=current_snapshot,
                    is_full_sync=False,
                    inserts=inserts,
                    updates=updates,
                    deletes=deletes,
                )

    logger.info(
        "Table {target} has been successfully synced with {source}, records changed: {records}",
        target=target_path.table,
        source=source_path.table,
        records=total_synced_records,
    )
