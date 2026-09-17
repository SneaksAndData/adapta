"""Iceberg reader (via REST Catalog)"""

import os
from typing import Literal

import polars
import pyarrow.dataset
import pyiceberg
from polars import Expr, LazyFrame
from pyarrow.lib import Schema
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import ALWAYS_TRUE
from pyiceberg.table import Table as IcebergTable

from adapta.storage.iceberg.v1._models import IcebergRestCatalogConfig
from adapta.storage.models.expression_dsl.filter_expression import (
    Expression,
    compile_expression,
)
from adapta.storage.models.expression_dsl.iceberg_filter_expression import (
    IcebergFilterExpression,
)
from adapta.utils.metaframe import MetaFrame


def get_default_catalog(catalog_config: IcebergRestCatalogConfig) -> Catalog:
    """
    Loads a provided configuration as `default` Iceberg catalog
    """
    return load_catalog("default", **catalog_config.get_constructor_args)


def get_catalog(name: str, catalog_config: IcebergRestCatalogConfig) -> Catalog:
    """
    Loads a provided configuration as named catalog, using a provided name.
    """
    return load_catalog(name, **catalog_config.get_constructor_args)


def _apply_s3_endpoint_override(table: IcebergTable) -> IcebergTable:
    if "ADAPTA__ICEBERG_REST_CATALOG__S3_ENDPOINT_OVERRIDE" in os.environ:
        # FileIO's endpoint is taken directly from the catalog response
        # In case it differs from `s3.endpoint` set in catalog config, align them
        # Note that when vended credentials are used, table.config will take preference over client setting
        # thus endpoint is updated after catalog returns creds
        # this is necessary if your S3 service has multiple endpoints and client doesn't have access to the one used by catalog
        table.io.properties["s3.endpoint"] = os.environ["ADAPTA__ICEBERG_REST_CATALOG__S3_ENDPOINT_OVERRIDE"]
        table.config["s3.endpoint"] = os.environ["ADAPTA__ICEBERG_REST_CATALOG__S3_ENDPOINT_OVERRIDE"]

    return table


def load_using_catalog(
    schema: str,
    table_name: str,
    catalog: Catalog,
    row_filter: Expression | None = None,
    columns: tuple[str, ...] | None = None,
    limit: int | None = None,
    version_id: int | None = None,
    lazy_read: bool = False,
) -> MetaFrame:
    """
    Loads an Iceberg table as a Metaframe, using provided catalog connection

    :param schema: table schema name
    :param table_name: table name
    :param catalog: Iceberg catalog to use
    :param row_filter: Filter expression to be applied to a scan
    :param columns: Columns to return. If empty, all columns are returned.
    :param limit: Maximum number of rows to return. If empty, all rows are returned.
    :param version_id: Iceberg table version. If empty, reads the latest version,
    :param lazy_read: If true, returns Polars (LazyFrame) only convertable Metaframe
    """
    row_filter_expression = None

    if row_filter:
        row_filter_expression = compile_expression(row_filter, IcebergFilterExpression)

    table = catalog.load_table(identifier=(schema, table_name))
    if "ADAPTA__ICEBERG_REST_CATALOG__S3_ENDPOINT_OVERRIDE" in os.environ:
        # FileIO's endpoint is taken directly from the catalog response
        # In case it differs from `s3.endpoint` set in catalog config, align them
        # Note that when vended credentials are used, table.config will take preference over client setting
        # thus endpoint is updated after catalog returns creds
        # this is necessary if your S3 service has multiple endpoints and client doesn't have access to the one used by catalog
        table.io.properties["s3.endpoint"] = os.environ["ADAPTA__ICEBERG_REST_CATALOG__S3_ENDPOINT_OVERRIDE"]
        table.config["s3.endpoint"] = os.environ["ADAPTA__ICEBERG_REST_CATALOG__S3_ENDPOINT_OVERRIDE"]

    scanner = table.scan(
        row_filter=row_filter_expression or ALWAYS_TRUE,
        selected_fields=columns or ("*",),
        limit=limit,
        snapshot_id=version_id,
    )

    if lazy_read:
        return MetaFrame(
            data=scanner.to_arrow_batch_reader(),
            convert_to_polars=lambda v: polars.scan_pyarrow_dataset(pyarrow.dataset.dataset(v)),
            convert_to_pandas=None,
        )

    return MetaFrame(
        data=scanner,
        # use built-in DataScan converters
        convert_to_polars=lambda v: v.to_polars(),
        convert_to_pandas=lambda v: v.to_pandas(),
    )


def load_using_native_scan(
    schema: str,
    table_name: str,
    catalog: Catalog,
    version_id: int | None = None,
    reader_override: Literal["native", "pyiceberg"] = "pyiceberg",  # only use 'native' for AWS S3 backends
) -> MetaFrame:
    """
    Loads an Iceberg table as a Metaframe, using native Polars Scanner. Field filtering and row expressions are not supported.
    This method relies on **UNSTABLE** API to ensure compatibility with S3 implementations outside AWS.
    Use of `load_using_catalog` is recommended for production applications.
    """
    table = _apply_s3_endpoint_override(catalog.load_table(identifier=(schema, table_name)))

    return MetaFrame(
        table,
        # use built-in DataScan converters
        convert_to_polars=lambda table: polars.scan_iceberg(
            table,
            # Polars documentation considers this an unstable parameter.
            # In case lazyframe read fails, consider patching this to whatever will be supported
            reader_override=reader_override,
            snapshot_id=version_id,
            storage_options=table.config,
        ),
        convert_to_pandas=None,
    )


def collect_lazy_schema(data: polars.LazyFrame) -> Schema:
    """
    Read schema of a LazyFrame w/o materializing rows
    """
    polars_schema = data.collect_schema()
    return polars.DataFrame(schema=polars_schema).to_arrow().schema


def write_using_catalog(
    schema_name: str,
    table_name: str,
    catalog: Catalog,
    data: polars.DataFrame | polars.LazyFrame,
    write_chunk_size: int = 50_000,
    overwrite: bool = True,
    merge_columns: list[str] | None = None,
    delete_filter: Expression | None = None,
) -> None:
    """
    Writes data to an Iceberg table from the provided Metaframe. Will create a table if it doesn't exist.
    Data is written in chunks to regulate memory usage.
    If `merge_columns` is provided an upsert will be performed
    If `delete_filter` is provided the data matching the filter will be deleted before performing the insert/update
    Note when using S3 compatible storage: if you are getting checksum validation errors, add these two env variables:
        os.environ["AWS_REQUEST_CHECKSUM_CALCULATION"] = "WHEN_REQUIRED"
        os.environ["AWS_RESPONSE_CHECKSUM_VALIDATION"] = "WHEN_REQUIRED"
    """

    def _get_table(table_schema: Schema) -> pyiceberg.table.Table:
        if catalog.table_exists(identifier=(schema_name, table_name)):
            return catalog.load_table(identifier=(schema_name, table_name))

        return catalog.create_table(
            identifier=(schema_name, table_name),
            schema=table_schema,
        )

    def _get_schema() -> Schema:
        if isinstance(data, polars.DataFrame):
            return data.to_arrow().schema

        return collect_lazy_schema(data)

    target_table: pyiceberg.table.Table = _apply_s3_endpoint_override(_get_table(_get_schema()))

    delete_filter_expression = compile_expression(delete_filter, IcebergFilterExpression) if delete_filter else None

    with target_table.transaction() as write_tx:
        if overwrite:
            write_tx.delete(delete_filter=ALWAYS_TRUE)
        if delete_filter_expression:
            write_tx.delete(delete_filter=delete_filter_expression)
        iterator = (
            data.iter_slices(n_rows=write_chunk_size)
            if isinstance(data, polars.DataFrame)
            else data.collect_batches(chunk_size=write_chunk_size)
        )
        for data_chunk in iterator:
            if merge_columns:
                write_tx.upsert(data_chunk.to_arrow(), join_cols=merge_columns)
            else:
                write_tx.append(data_chunk.to_arrow())


def get_changes(
    schema_name: str,
    table_name: str,
    catalog: Catalog,
    from_snapshot_id: int,
    to_snapshot_id: int,
    tracking_column: str,
    primary_key_columns: tuple[str, ...],
) -> tuple[
    LazyFrame,
    LazyFrame,
    LazyFrame,
]:
    """
    Retrieve changes between two snapshots as a MetaFrame
    """

    def _not_null_expr(suffix: str, fields: tuple[str, ...]) -> Expr:
        expr_base = polars.lit(1).eq(1)
        for field in fields:
            if suffix:
                expr_base = expr_base & polars.col(f"{field}_{suffix}").is_not_null()
            else:
                expr_base = expr_base & polars.col(field).is_not_null()

        return expr_base

    def _null_expr(suffix: str, fields: tuple[str, ...]) -> Expr:
        expr_base = polars.lit(1).eq(1)
        for field in fields:
            if suffix:
                expr_base = expr_base & polars.col(f"{field}_{suffix}").is_null()
            else:
                expr_base = expr_base & polars.col(field).is_null()

        return expr_base

    current_version: LazyFrame = load_using_catalog(
        schema_name,
        table_name,
        catalog,
        columns=(tracking_column, *primary_key_columns),
        version_id=to_snapshot_id,
        lazy_read=True,
    ).to_polars()
    previous_version: LazyFrame = load_using_catalog(
        schema_name,
        table_name,
        catalog,
        columns=(tracking_column, *primary_key_columns),
        version_id=from_snapshot_id,
        lazy_read=True,
    ).to_polars()
    current_version_full: LazyFrame = load_using_catalog(
        schema_name,
        table_name,
        catalog,
        version_id=to_snapshot_id,
        lazy_read=True,
    ).to_polars()

    diff_table = current_version.join(
        previous_version,
        on=primary_key_columns,
        how="outer",
    )

    # Inserts: pk exists now, but didn't exist previously
    inserts = current_version_full.join(
        diff_table.filter(_null_expr("right", primary_key_columns)), on=primary_key_columns, how="semi"
    )

    # Updates: pk exists in both, pick latest
    updates = diff_table.filter(
        _not_null_expr("", primary_key_columns)
        & _not_null_expr("right", primary_key_columns)
        & (polars.col(tracking_column) > polars.col(f"{tracking_column}_right"))
    ).drop(polars.selectors.contains("_right"))

    # Deletes: pk existed previously, but doesn't exist now
    deletes = (
        diff_table.filter(_null_expr("", primary_key_columns))
        .drop(~polars.selectors.contains("_right"))
        .rename({f"{k}_right": k for k in primary_key_columns})
    )

    return inserts, updates, deletes


def set_property(
    schema_name: str,
    table_name: str,
    catalog: Catalog,
    property_name: str,
    property_value: str,
) -> None:
    """
    Sets or updates a custom property on the specified Iceberg table
    """
    table = _apply_s3_endpoint_override(catalog.load_table(identifier=(schema_name, table_name)))

    with table.transaction() as tx:
        tx.set_properties(
            {
                property_name: property_value,
            }
        )


def get_property(
    schema_name: str,
    table_name: str,
    catalog: Catalog,
    property_name: str,
) -> str | None:
    """
    Retrieves a custom property from the specified Iceberg table
    """
    table = _apply_s3_endpoint_override(catalog.load_table(identifier=(schema_name, table_name)))
    return table.properties.get(property_name, None)


def get_schema(
    schema_name: str,
    table_name: str,
    catalog: Catalog,
) -> IcebergSchema:
    """
    Retrieves a schema of the table
    """
    table = _apply_s3_endpoint_override(catalog.load_table(identifier=(schema_name, table_name)))
    return table.schema()


def get_current_snapshot(
    schema_name: str,
    table_name: str,
    catalog: Catalog,
) -> int:
    """
    Retrieves a latest snapshot of the table
    """
    table = _apply_s3_endpoint_override(catalog.load_table(identifier=(schema_name, table_name)))
    return table.current_snapshot().snapshot_id
