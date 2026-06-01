"""Iceberg reader (via REST Catalog)"""
from typing import Iterator

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
    version_id: str | None = None,
    lazy_read: bool = False,
) -> MetaFrame | Iterator[MetaFrame]:
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

    if lazy_read:
        return MetaFrame(
            data=catalog.load_table(identifier=(schema, table_name)),
            # use built-in DataScan converters
            convert_to_polars=lambda v: v.to_polars(),
            convert_to_pandas=None,
        )

    scanner = catalog.load_table(identifier=(schema, table_name)).scan(
        row_filter=row_filter_expression or ALWAYS_TRUE,
        selected_fields=columns or "*",
        limit=limit,
        snapshot_id=version_id,
    )

    return MetaFrame(
        data=scanner,
        # use built-in DataScan converters
        convert_to_polars=lambda v: v.to_polars(),
        convert_to_pandas=lambda v: v.to_pandas(),
    )
