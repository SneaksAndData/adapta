from typing import Iterator

from pyiceberg.table import StaticTable

from adapta.storage.iceberg.v1 import IcebergRestCatalog
from adapta.storage.models import DataPath
from adapta.storage.models.expression_dsl.filter_expression import compile_expression, Expression
from adapta.storage.models.expression_dsl.arrow_filter_expression import ArrowFilterExpression
from adapta.utils.metaframe import MetaFrame


from pyiceberg.catalog import load_catalog


def load_using_catalog(
    schema: str,
    table_name: str,
    row_filter: Expression | None = None,
    columns: list[str] | None = None,
    limit: int = None,
) -> MetaFrame | Iterator[MetaFrame]:
    """
    Loads an Iceberg table as Metaframe using catalog
    """
    catalog = load_catalog("default", **IcebergRestCatalog.create("", "").get_constructor_args)

    if row_filter:
        row_filter_expression = compile_expression(row_filter, ArrowFilterExpression)

    table_ref = catalog.load_table(identifier=(schema, table_name)).scan()

    return MetaFrame(
        data=table_ref,
        convert_to_polars=lambda v: v.to_polars(),
        convert_to_pandas=lambda v: v.scan().to_pandas(),
    )


def load_using_path(path: DataPath) -> MetaFrame | Iterator[MetaFrame]:
    """
    Loads an Iceberg table as Metaframe using storage path
    """
    static_table = StaticTable.from_metadata(path.to_hdfs_path())

    return MetaFrame(
        data=static_table,
        convert_to_polars=lambda v: v.to_polars(),
        convert_to_pandas=lambda v: v.scan().to_pandas(),
    )
