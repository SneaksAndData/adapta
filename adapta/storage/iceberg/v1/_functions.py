"""Iceberg reader (via REST Catalog)"""
import os
from typing import Literal

import polars
import pyarrow.dataset
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.table import ALWAYS_TRUE

from adapta.storage.iceberg.v1._models import IcebergRestCatalogConfig
from adapta.storage.models.expression_dsl.filter_expression import compile_expression, Expression
from adapta.storage.models.expression_dsl.iceberg_filter_expression import IcebergFilterExpression
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


def load_using_catalog(
    schema: str,
    table_name: str,
    catalog: Catalog,
    row_filter: Expression | None = None,
    columns: tuple[str] | None = None,
    limit: int = None,
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

    if lazy_read:
        return MetaFrame(
            data=table.scan(
                row_filter=row_filter_expression or ALWAYS_TRUE,
                selected_fields=columns or ("*",),
                limit=limit,
                snapshot_id=version_id,
            ).to_arrow_batch_reader(),
            convert_to_polars=lambda v: polars.scan_pyarrow_dataset(pyarrow.dataset.dataset(v)),
            convert_to_pandas=None,
        )

    scanner = table.scan(
        row_filter=row_filter_expression or ALWAYS_TRUE,
        selected_fields=columns or ("*",),
        limit=limit,
        snapshot_id=version_id,
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
    return MetaFrame(
        data=catalog.load_table(identifier=(schema, table_name)),
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
