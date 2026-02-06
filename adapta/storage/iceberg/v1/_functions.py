from typing import Iterator

from pyiceberg.table import StaticTable, ALWAYS_TRUE

from adapta.storage.iceberg.v1 import IcebergRestCatalog
from adapta.storage.models import DataPath
from adapta.storage.models.expression_dsl.filter_expression import compile_expression, Expression
from adapta.storage.models.expression_dsl.iceberg_filter_expression import IcebergFilterExpression
from adapta.utils.metaframe import MetaFrame


from pyiceberg.catalog import load_catalog


def load_using_catalog(
    schema: str,
    table_name: str,
    row_filter: Expression | None = None,
    columns: tuple[str] | None = None,
    limit: int = None,
    version_id: str | None = None,
) -> MetaFrame | Iterator[MetaFrame]:
    """
    Loads an Iceberg table as Metaframe using catalog
    """
    catalog = load_catalog("default", **IcebergRestCatalog.create("", "").get_constructor_args)
    row_filter_expression = None

    if row_filter:
        row_filter_expression = compile_expression(row_filter, IcebergFilterExpression)

    scanner = catalog.load_table(identifier=(schema, table_name)).scan(
        row_filter=row_filter_expression or ALWAYS_TRUE,
        selected_fields=columns or "*",
        limit=limit,
        snapshot_id=version_id,
    )

    return MetaFrame(
        data=scanner,
        convert_to_polars=lambda v: v.to_polars(),
        convert_to_pandas=lambda v: v.to_pandas(),
    )


def load_using_path(
    path: DataPath,
    row_filter: Expression | None = None,
    columns: tuple[str] | None = None,
    limit: int = None,
    version_id: str | None = None,
) -> MetaFrame | Iterator[MetaFrame]:
    """
    Loads an Iceberg table as Metaframe using storage path
    """
    row_filter_expression = None
    if row_filter:
        row_filter_expression = compile_expression(row_filter, IcebergFilterExpression)

    scanner = StaticTable.from_metadata(path.to_hdfs_path()).scan(
        row_filter=row_filter_expression or ALWAYS_TRUE,
        selected_fields=columns or "*",
        limit=limit,
        snapshot_id=version_id,
    )

    return MetaFrame(
        data=scanner,
        convert_to_polars=lambda v: v.to_polars(),
        convert_to_pandas=lambda v: v.to_pandas(),
    )
