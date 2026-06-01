import polars
import sqlalchemy
from polars.testing import assert_frame_equal
from pyiceberg.catalog import Catalog

from adapta.storage.iceberg.v1 import load_using_catalog
from tests.iceberg_clients._functions import prepare_iceberg_table, get_input_data


def test_simple_read(trino_test_connection: sqlalchemy.engine.Engine, iceberg_catalog: Catalog):
    input_data = get_input_data()
    expected_pl = polars.DataFrame(input_data)
    prepare_iceberg_table(
        "test_simple_read",
        data=input_data,
        trino_test_connection=trino_test_connection,
    )

    data = load_using_catalog(
        schema="test",
        table_name="test_simple_read",
        catalog=iceberg_catalog,
    )

    assert_frame_equal(data.to_polars().sort("cola"), expected_pl.sort("cola"), check_column_order=False)


def test_lazy_read(trino_test_connection: sqlalchemy.engine.Engine, iceberg_catalog: Catalog):
    input_data = get_input_data()
    expected_pl = polars.DataFrame(input_data)
    prepare_iceberg_table(
        "test_lazy_read",
        data=input_data,
        trino_test_connection=trino_test_connection,
    )

    data = (
        load_using_catalog(schema="test", table_name="test_lazy_read", catalog=iceberg_catalog, lazy_read=True)
        .to_polars()
        .collect()
    )

    assert_frame_equal(data.sort("cola"), expected_pl.sort("cola"), check_column_order=False)
