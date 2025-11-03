from typing import Iterator

from pyiceberg.table import StaticTable

from adapta.storage.iceberg.v1 import IcebergRestCatalog
from adapta.storage.models import DataPath
from adapta.utils.metaframe import MetaFrame


from pyiceberg.catalog import load_catalog

def load_using_catalog(schema: str, table_name: str) -> MetaFrame | Iterator[MetaFrame]:
    catalog = load_catalog(
        "default",
        **IcebergRestCatalog.create("", "").get_constructor_args
    )

    table_ref = catalog.load_table(identifier=(schema, table_name))

    return MetaFrame(
        data=table_ref,
        convert_to_polars=lambda v: v.to_polars(),
        convert_to_pandas=lambda v: v.scan().to_pandas(),

    )

def load_using_path(path: DataPath) -> MetaFrame | Iterator[MetaFrame]:
    static_table = StaticTable.from_metadata(path.to_hdfs_path())

    return MetaFrame(
        data=static_table,
        convert_to_polars=lambda v: v.to_polars(),
        convert_to_pandas=lambda v: v.scan().to_pandas(),

    )
