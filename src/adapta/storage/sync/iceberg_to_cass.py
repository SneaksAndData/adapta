import dataclasses
from dataclasses import dataclass
from enum import Enum
from typing import Self, final

from polars import LazyFrame
from pyiceberg.catalog import Catalog

from adapta.logs import LoggerInterface
from adapta.process_communication import DataSocket
from adapta.storage.distributed_object_store.v3.cassandra_client import CassandraClient, TCassandraModel, get_mapper
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


@final
@dataclass
class CustomIndexMetadata:
    """
    Metadata describing a custom index table in Iceberg and Cassandra.
    """

    index_model: type[TCassandraModel]
    field_name: str
    iceberg_table: str
    cassandra_table: str
    index_columns: list[str]

    @classmethod
    def create(
        cls,
        base_model: type[dataclass],
        index_field: dataclasses.Field,
        source_table: str,
        target_table: str,
        version_field: str,
    ) -> Self:
        model_fields = {f.name: f for f in dataclasses.fields(base_model)}
        orig_pks = [f for f in dataclasses.fields(base_model) if f.metadata.get("is_primary_key", False)]
        ver_field = model_fields.get(version_field)

        fields_spec = [
            (
                index_field.name,
                index_field.type,
                dataclasses.field(metadata={"is_primary_key": True, "is_partition_key": True}),
            )
        ]
        for pk in orig_pks:
            if pk.name != index_field.name:
                fields_spec.append((pk.name, pk.type, dataclasses.field()))
        if ver_field and ver_field.name != index_field.name:
            fields_spec.append((ver_field.name, ver_field.type, dataclasses.field()))

        index_model = dataclasses.make_dataclass(f"{base_model.__name__}__idx_{index_field.name}", fields_spec)
        index_cols = [f[0] for f in fields_spec]

        return cls(
            index_model=index_model,
            field_name=index_field.name,
            iceberg_table=f"{source_table}__idx_{index_field.name}",
            cassandra_table=f"{target_table}__idx_{index_field.name}",
            index_columns=index_cols,
        )


def _get_custom_index_models(
    cassandra_model: type[dataclass],
    source_table: str,
    target_table: str,
    version_field: str,
    logger: LoggerInterface,
) -> list[CustomIndexMetadata]:
    custom_idx_fields = [f for f in dataclasses.fields(cassandra_model) if f.metadata.get("is_custom_index", False)]
    if not custom_idx_fields:
        return []

    index_models: list[CustomIndexMetadata] = []
    for idx_f in custom_idx_fields:
        idx_meta = CustomIndexMetadata.create(
            base_model=cassandra_model,
            index_field=idx_f,
            source_table=source_table,
            target_table=target_table,
            version_field=version_field,
        )
        logger.info(
            "Identified custom index field {field_name} with columns {columns}",
            field_name=idx_meta.field_name,
            columns=idx_meta.index_columns,
        )
        index_models.append(idx_meta)

    return index_models


def _sync_custom_index(
    index_metadata: CustomIndexMetadata,
    source_schema: str,
    source_table: str,
    target_keyspace: str,
    version_field: str,
    iceberg_catalog: Catalog,
    client: CassandraClient,
    upload_mode: CassandraUploadMode,
    read_chunk_size: int,
    threads: int,
    logger: LoggerInterface,
) -> None:
    logger.info("Reading source table {source_table} for custom index", source_table=source_table)
    full_source: LazyFrame = load_using_catalog(
        source_schema,
        source_table,
        iceberg_catalog,
        lazy_read=True,
    ).to_polars()
    idx_data = full_source.select(index_metadata.index_columns)

    idx_exists = iceberg_catalog.table_exists(identifier=(source_schema, index_metadata.iceberg_table))
    idx_previous_snapshot = (
        get_current_snapshot(source_schema, index_metadata.iceberg_table, iceberg_catalog) if idx_exists else None
    )

    logger.info("Writing index data to Iceberg table {iceberg_table}", iceberg_table=index_metadata.iceberg_table)
    write_using_catalog(
        schema_name=source_schema,
        table_name=index_metadata.iceberg_table,
        catalog=iceberg_catalog,
        data=idx_data,
        overwrite=True,
    )

    idx_current_snapshot = get_current_snapshot(source_schema, index_metadata.iceberg_table, iceberg_catalog)

    def _upload_index(source: LazyFrame) -> None:
        if upload_mode == CassandraUploadMode.CONCURRENT_BATCH:
            client.upload_concurrent_batch(
                source=source,
                table_name=index_metadata.cassandra_table,
                entity_type=index_metadata.index_model,
                keyspace=target_keyspace,
                batch_size=read_chunk_size,
                threads=threads,
            )
            return
        if upload_mode == CassandraUploadMode.CONCURRENT_NATIVE:
            client.upload_concurrent_native(
                source=source,
                table_name=index_metadata.cassandra_table,
                entity_type=index_metadata.index_model,
                keyspace=target_keyspace,
                batch_size=read_chunk_size,
                threads=threads,
            )
            return
        raise ValueError(f"Unsupported upload mode: {upload_mode}")

    if not idx_previous_snapshot:
        logger.info(
            "Syncing index table {cassandra_table} to Cassandra", cassandra_table=index_metadata.cassandra_table
        )
        _upload_index(idx_data)
        return

    if idx_previous_snapshot == idx_current_snapshot:
        return

    logger.info(
        "Retrieving changes for index table {iceberg_table} between snapshots {prev_snap} and {curr_snap}",
        iceberg_table=index_metadata.iceberg_table,
        prev_snap=idx_previous_snapshot,
        curr_snap=idx_current_snapshot,
    )
    idx_inserts, idx_updates, idx_deletes = get_changes(
        schema_name=source_schema,
        table_name=index_metadata.iceberg_table,
        catalog=iceberg_catalog,
        from_snapshot_id=idx_previous_snapshot,
        to_snapshot_id=idx_current_snapshot,
        tracking_column=version_field,
        primary_key_columns=(index_metadata.field_name,),
    )

    if idx_inserts is not None:
        logger.info(
            "Syncing index inserts to Cassandra table {cassandra_table}",
            cassandra_table=index_metadata.cassandra_table,
        )
        _upload_index(idx_inserts)

    if idx_updates is not None:
        logger.info(
            "Syncing index updates to Cassandra table {cassandra_table}",
            cassandra_table=index_metadata.cassandra_table,
        )
        _upload_index(idx_updates)

    if idx_deletes is not None:
        logger.info(
            "Syncing index deletes to Cassandra table {cassandra_table}",
            cassandra_table=index_metadata.cassandra_table,
        )
        client.delete_batch_concurrent(
            source=idx_deletes,
            table_name=index_metadata.cassandra_table,
            entity_type=index_metadata.index_model,
            keyspace=target_keyspace,
            batch_size=read_chunk_size,
            threads=threads,
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
    current_snapshot = get_current_snapshot(source_path.schema, source_path.table, iceberg_catalog)

    if not last_synced_snapshot or last_synced_snapshot == "-1":
        logger.info("Last sync snapshot data not available. Will perform a full sync.")
        source_data: LazyFrame = load_using_catalog(
            source_path.schema,
            source_path.table,
            iceberg_catalog,
            lazy_read=True,
        ).to_polars()
        total_synced_records = _sync_lazyframe(source_data)
    elif int(last_synced_snapshot) != current_snapshot:
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
    else:
        logger.info(
            "Table {target} is already up to date with {source} at snapshot {snapshot}",
            target=target_path.table,
            source=source_path.table,
            snapshot=current_snapshot,
        )
        return

    set_property(
        source_path.schema,
        source_path.table,
        iceberg_catalog,
        "adapta.cassandra.last-synced-snapshot-id",
        str(current_snapshot),
    )
    for idx_meta in custom_index_models:
        _sync_custom_index(
            index_metadata=idx_meta,
            source_schema=source_path.schema,
            source_table=source_path.table,
            target_keyspace=target_path.keyspace,
            version_field=version_field,
            iceberg_catalog=iceberg_catalog,
            client=client,
            upload_mode=upload_mode,
            read_chunk_size=read_chunk_size,
            threads=threads,
            logger=logger,
        )

    logger.info(
        "Table {target} has been successfully synced with {source}, records changed: {records}",
        target=target_path.table,
        source=source_path.table,
        records=total_synced_records,
    )
