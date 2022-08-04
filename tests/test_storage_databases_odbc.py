import pytest
import pandas
from proteus.storage.database.odbc import OdbcClient
from proteus.storage.database.models import WriteMode


def sku_data():
    return pandas.DataFrame(data={
        'sku_id': ["1", "2", "3"],
        'sku_name': ["Exostrike", "BIOM", "Collin"],
        'location_id': ["1", "1", "2"],
        'cost': [100.0, 50.2, 40.6]
    })


def location_data():
    return pandas.DataFrame(data={
        'location_id': ["1", "2", "3"],
        'location_name': ["Østergade", "Bredebro", "Købmagergade"],
    })


def test_materialize(sqlite: OdbcClient):
    """
    Test that writing a table and reading it again will return the original dataframe.
    """
    with sqlite:
        _ = sqlite.materialize(
            data=sku_data(),
            schema='main',
            name='sku',
            write_mode=WriteMode.REPLACE
        )

        result = sqlite.query("SELECT * FROM main.sku")

    assert result.equals(sku_data())


def test_read_non_existing_table(sqlite: OdbcClient):
    """
    Test that the method returns None if a non-existing table is attempted to be read from.
    """
    with sqlite:
        result = sqlite.query("SELECT * FROM main.product")

    assert result is None


def test_write_empty_schema(sqlite: OdbcClient):
    """
    Test that the method returns None if a non-existing table is attempted to be written to.
    """
    with sqlite:
        result = sqlite.materialize(data=pandas.DataFrame(data={}), schema="main", name="product", write_mode=WriteMode.REPLACE)

    assert result is None


def test_joined_write_read_frame(sqlite: OdbcClient):
    """
    Test that writing two tables and reading them joined again will return the original dataframes joined as well.
    """
    with sqlite:
        _ = sqlite.materialize(
            data=sku_data(),
            schema="main",
            name="sku",
            write_mode=WriteMode.REPLACE
        )

        _ = sqlite.materialize(
            data=location_data(),
            schema="main",
            name="location",
            write_mode=WriteMode.REPLACE
        )

        result = sqlite.query("""
          SELECT 
             sku_name, 
             location_name, 
             cost 
          FROM 
             main.sku 
             INNER JOIN main.location ON sku.location_id = location.location_id""")

    assert result.equals(
        sku_data().merge(
            location_data(), how='inner', on='location_id'
        )[['sku_name', 'location_name', 'cost']]
    )


def test_write_append(sqlite: OdbcClient):
    """
    Test that writing two tables with append and reading it again will return the original dataframes appended to
    each other.
    """
    with sqlite:
        sqlite.materialize(data=sku_data(), schema="main", name="sku", write_mode=WriteMode.REPLACE)
        sqlite.materialize(data=sku_data(), schema="main", name="sku", write_mode=WriteMode.APPEND)

        result = sqlite.query("SELECT * FROM main.sku")

    assert result.equals(sku_data().append(sku_data()).reset_index(drop=True))


def test_write_replace(sqlite: OdbcClient):
    """
    Test that writing two tables with replace and reading it again will return the last written dataframe.
    """
    with sqlite:
        sqlite.materialize(data=sku_data(), schema="main", name="sku", write_mode=WriteMode.REPLACE)
        sku_df2 = sku_data()
        sku_df2['location_id'] = '4'
        sqlite.materialize(data=sku_df2, schema="main", name="sku", write_mode=WriteMode.REPLACE)

        result = sqlite.query("SELECT * FROM main.sku")

    assert result.equals(sku_df2)


def test_write_truncate(sqlite: OdbcClient):
    """
    Test that writing two tables with truncate and reading it again will return the last written dataframe.
    """
    with sqlite:
        sku_df = sku_data()
        sqlite.materialize(data=sku_data(), schema="main", name="sku", write_mode=WriteMode.APPEND)
        sku_df2 = sku_df.copy()
        sku_df2['location_id'] = '4'
        sqlite.materialize(data=sku_df2, schema="main", name="sku", write_mode=WriteMode.TRUNCATE)

        read_df = sqlite.query("SELECT * FROM main.sku")

    assert read_df.equals(sku_df2)
