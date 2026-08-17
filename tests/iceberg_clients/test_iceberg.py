import random

import polars
import pytest
import sqlalchemy
from polars.testing import assert_frame_equal
from pyiceberg.catalog import Catalog
from sqlalchemy import text

from adapta.storage.iceberg.v1 import load_using_catalog, write_using_catalog
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
        con.execute(text("""
        CREATE OR REPLACE TABLE test.test_map_read (
            cola integer,
            colb varchar,
            colc array(integer),
            cold map(varchar(10), double)
        )"""))
        for ix_row in range(len(input_data["cola"])):
            array_value = ", ".join([str(v) for v in input_data["colc"][ix_row]])
            map_keys_value = ", ".join([f"'{v['key']}'" for v in input_data["cold"][ix_row]])
            map_values_value = ", ".join([str(v["value"]) for v in input_data["cold"][ix_row]])
            query = text(f"""
                         INSERT INTO test.test_map_read (cola, colb, colc, cold)
                         VALUES ({input_data['cola'][ix_row]}, '{input_data['colb'][ix_row]}', ARRAY[{array_value}], MAP(ARRAY[{map_keys_value}], cast(ARRAY[{map_values_value}] as array(double))))
                         """)
            con.execute(query)

    data = load_using_catalog(
        schema="test",
        table_name="test_map_read",
        catalog=iceberg_catalog,
    )

    assert_frame_equal(data.to_polars().sort("cola"), expected_pl.sort("cola"), check_column_order=False)


@pytest.mark.parametrize("lazy", [False, True])
def test_create_table_from_df(iceberg_catalog: Catalog, lazy: bool):
    table_name = f"test_create_table_{generate_random_string(8)}".lower()
    input_data = get_input_data()
    df = polars.DataFrame(input_data)
    data_to_write = df.lazy() if lazy else df

    write_using_catalog(
        schema_name="test",
        table_name=table_name,
        catalog=iceberg_catalog,
        data=data_to_write,
        overwrite=True,
    )

    read_data = load_using_catalog(
        schema="test",
        table_name=table_name,
        catalog=iceberg_catalog,
    )
    assert_frame_equal(read_data.to_polars().sort("cola"), df.sort("cola"), check_column_order=False)


@pytest.mark.parametrize("lazy", [False, True])
def test_overwrite_table_with_df(iceberg_catalog: Catalog, lazy: bool):
    table_name = f"test_overwrite_table_{generate_random_string(8)}".lower()
    input_data1 = {
        "cola": [1, 2, 3],
        "colb": ["a", "b", "c"],
        "colc": [[1], [2], [3]],
    }
    df1 = polars.DataFrame(input_data1)
    data1_to_write = df1.lazy() if lazy else df1

    write_using_catalog(
        schema_name="test",
        table_name=table_name,
        catalog=iceberg_catalog,
        data=data1_to_write,
        overwrite=True,
    )

    input_data2 = {
        "cola": [4, 5],
        "colb": ["x", "y"],
        "colc": [[4], [5]],
    }
    df2 = polars.DataFrame(input_data2)
    data2_to_write = df2.lazy() if lazy else df2

    write_using_catalog(
        schema_name="test",
        table_name=table_name,
        catalog=iceberg_catalog,
        data=data2_to_write,
        overwrite=True,
    )

    read_data = load_using_catalog(
        schema="test",
        table_name=table_name,
        catalog=iceberg_catalog,
    )
    assert_frame_equal(read_data.to_polars().sort("cola"), df2.sort("cola"), check_column_order=False)


@pytest.mark.parametrize("lazy", [False, True])
def test_append_to_table(iceberg_catalog: Catalog, lazy: bool):
    table_name = f"test_append_table_{generate_random_string(8)}".lower()
    input_data1 = {
        "cola": [1, 2],
        "colb": ["a", "b"],
        "colc": [[1], [2]],
    }
    df1 = polars.DataFrame(input_data1)
    data1_to_write = df1.lazy() if lazy else df1

    write_using_catalog(
        schema_name="test",
        table_name=table_name,
        catalog=iceberg_catalog,
        data=data1_to_write,
        overwrite=True,
    )

    input_data2 = {
        "cola": [3, 4],
        "colb": ["c", "d"],
        "colc": [[3], [4]],
    }
    df2 = polars.DataFrame(input_data2)
    data2_to_write = df2.lazy() if lazy else df2

    write_using_catalog(
        schema_name="test",
        table_name=table_name,
        catalog=iceberg_catalog,
        data=data2_to_write,
        overwrite=False,
    )

    expected_df = polars.concat([df1, df2])

    read_data = load_using_catalog(
        schema="test",
        table_name=table_name,
        catalog=iceberg_catalog,
    )
    assert_frame_equal(read_data.to_polars().sort("cola"), expected_df.sort("cola"), check_column_order=False)


@pytest.mark.parametrize("lazy", [False, True])
def test_upsert_to_table(iceberg_catalog: Catalog, lazy: bool):
    table_name = f"test_upsert_table_{generate_random_string(8)}".lower()
    input_data1 = {
        "cola": [1, 2],
        "colb": ["a", "b"],
        "colc": [[1], [2]],
    }
    df1 = polars.DataFrame(input_data1)
    data1_to_write = df1.lazy() if lazy else df1

    write_using_catalog(
        schema_name="test",
        table_name=table_name,
        catalog=iceberg_catalog,
        data=data1_to_write,
        overwrite=True,
    )

    input_data2 = {
        "cola": [1, 3],
        "colb": ["aa", "c"],
        "colc": [[11], [3]],
    }
    df2 = polars.DataFrame(input_data2)
    data2_to_write = df2.lazy() if lazy else df2

    write_using_catalog(
        schema_name="test",
        table_name=table_name,
        catalog=iceberg_catalog,
        data=data2_to_write,
        overwrite=False,
        merge_columns=["cola"],
    )

    expected_df = polars.DataFrame(
        {
            "cola": [1, 2, 3],
            "colb": ["aa", "b", "c"],
            "colc": [[11], [2], [3]],
        }
    )

    read_data = load_using_catalog(
        schema="test",
        table_name=table_name,
        catalog=iceberg_catalog,
    )
    assert_frame_equal(read_data.to_polars().sort("cola"), expected_df.sort("cola"), check_column_order=False)
