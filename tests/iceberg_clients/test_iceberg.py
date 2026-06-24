import random

import polars
import sqlalchemy
from polars.testing import assert_frame_equal
from pyiceberg.catalog import Catalog
from sqlalchemy import text

from adapta.storage.iceberg.v1 import load_using_catalog
from tests.iceberg_clients._functions import prepare_iceberg_table, get_input_data, generate_random_string


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


def test_map_read(trino_test_connection: sqlalchemy.engine.Engine, iceberg_catalog: Catalog):
    input_data = get_input_data() | {
        "cold": list(
            [
                [{"key": "key1", "value": random.random() * 100}, {"key": "key2", "value": random.random() * 100}]
                for _ in range(10)
            ]
        ),
    }
    schema = {
        "cola": polars.Int32,
        "colb": polars.String,
        "colc": polars.List(polars.Int32),
        "cold": polars.List(polars.Struct({"key": polars.String, "value": polars.Float64})),
    }
    expected_pl = polars.DataFrame(input_data, schema=schema)

    with trino_test_connection.connect() as con:
        con.execute(
            text(
                """
        CREATE OR REPLACE TABLE test.test_map_read (
            cola integer,
            colb varchar,
            colc array(integer),
            cold map(varchar(10), double)
        )"""
            )
        )
        for ix_row in range(len(input_data["cola"])):
            array_value = ", ".join([str(v) for v in input_data["colc"][ix_row]])
            map_keys_value = ", ".join([f"'{v['key']}'" for v in input_data["cold"][ix_row]])
            map_values_value = ", ".join([str(v["value"]) for v in input_data["cold"][ix_row]])
            query = text(
                f"""
                         INSERT INTO test.test_map_read (cola, colb, colc, cold)
                         VALUES ({input_data['cola'][ix_row]}, '{input_data['colb'][ix_row]}', ARRAY[{array_value}], MAP(ARRAY[{map_keys_value}], cast(ARRAY[{map_values_value}] as array(double))))
                         """
            )
            con.execute(query)

    data = load_using_catalog(
        schema="test",
        table_name="test_map_read",
        catalog=iceberg_catalog,
    )

    assert_frame_equal(data.to_polars().sort("cola"), expected_pl.sort("cola"), check_column_order=False)
