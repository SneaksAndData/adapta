import sqlalchemy

from adapta.storage.iceberg.v1 import get_default_catalog, IcebergRestCatalogConfig, load_using_catalog
from tests.iceberg_clients._functions import prepare_iceberg_table


def test_simple_read(trino_test_connection: sqlalchemy.engine.Engine):
    prepare_iceberg_table(
        "test_simple_read",
        data={
            "colA": list(range(10)),
            "colB": list(range(10)),
        },
        trino_test_connection=trino_test_connection,
    )

    catalog = get_default_catalog(IcebergRestCatalogConfig.from_environment())
    data = load_using_catalog(
        schema="test",
        table_name="test_simple_read",
        catalog=catalog,
    )

    assert True
