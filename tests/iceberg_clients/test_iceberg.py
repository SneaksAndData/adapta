import polars
from polars.testing import assert_frame_equal
import sqlalchemy

from adapta.storage.iceberg.v1 import get_default_catalog, IcebergRestCatalogConfig, load_using_catalog
from tests.iceberg_clients._functions import prepare_iceberg_table


def test_simple_read(trino_test_connection: sqlalchemy.engine.Engine):
    input_data = {
        "cola": list(range(10)),
        "colb": list(range(10)),
    }
    expected_pl = polars.DataFrame(input_data)
    prepare_iceberg_table(
        "test_simple_read",
        data=input_data,
        trino_test_connection=trino_test_connection,
    )

    catalog = get_default_catalog(IcebergRestCatalogConfig.from_environment(oauth2_enabled=False))
    data = load_using_catalog(
        schema="test",
        table_name="test_simple_read",
        catalog=catalog,
    )

    assert_frame_equal(data.to_polars().sort("cola"), expected_pl.sort("cola"), check_column_order=False)
