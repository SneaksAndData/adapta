import pandas

from proteus.storage.database.odbc import OdbcClient


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


def test_simple_write_read_frame(sqlite: OdbcClient):
    """
    Test that writing a table and reading it again will return the original dataframe.
    """
    with sqlite:
        _ = sqlite.materialize(
            data=sku_data(),
            schema='main',
            name='sku',
            overwrite=True
        )

        result = sqlite.query("SELECT * FROM main.sku")

    assert result.equals(sku_data())
