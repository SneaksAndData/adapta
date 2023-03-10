#  Copyright (c) 2023. ECCO Sneaks & Data
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import pandas
from adapta.storage.database.odbc import OdbcClient


def sku_data():
    return pandas.DataFrame(
        data={
            "sku_id": ["1", "2", "3"],
            "sku_name": ["Exostrike", "BIOM", "Collin"],
            "location_id": ["1", "1", "2"],
            "cost": [100.0, 50.2, 40.6],
        }
    )


def location_data():
    return pandas.DataFrame(
        data={
            "location_id": ["1", "2", "3"],
            "location_name": ["Østergade", "Bredebro", "Købmagergade"],
        }
    )


def test_materialize(sqlite: OdbcClient):
    """
    Test that writing a table and reading it again will return the original dataframe.
    """
    with sqlite:
        _ = sqlite.materialize(
            data=sku_data(),
            schema="main",
            name="sku",
            overwrite=True,
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
        result = sqlite.materialize(
            data=pandas.DataFrame(data={}),
            schema="main",
            name="product",
            overwrite=True,
        )

    assert result is None


def test_joined_write_read_frame(sqlite: OdbcClient):
    """
    Test that writing two tables and reading them joined again will return the original dataframes joined as well.
    """
    with sqlite:
        _ = sqlite.materialize(
            data=sku_data(), schema="main", name="sku", overwrite=True
        )

        _ = sqlite.materialize(
            data=location_data(), schema="main", name="location", overwrite=True
        )

        result = sqlite.query(
            """
          SELECT 
             sku_name, 
             location_name, 
             cost 
          FROM 
             main.sku 
             INNER JOIN main.location ON sku.location_id = location.location_id"""
        )

    assert result.equals(
        sku_data().merge(location_data(), how="inner", on="location_id")[
            ["sku_name", "location_name", "cost"]
        ]
    )


def test_write_append(sqlite: OdbcClient):
    """
    Test that writing two tables with append and reading it again will return the original dataframes appended to
    each other.
    """
    with sqlite:
        sqlite.materialize(data=sku_data(), schema="main", name="sku", overwrite=True)
        sqlite.materialize(data=sku_data(), schema="main", name="sku", overwrite=False)

        result = sqlite.query("SELECT * FROM main.sku")

    assert result.equals(sku_data().append(sku_data()).reset_index(drop=True))


def test_write_replace(sqlite: OdbcClient):
    """
    Test that writing two tables with replace and reading it again will return the last written dataframe.
    """
    with sqlite:
        sqlite.materialize(data=sku_data(), schema="main", name="sku", overwrite=True)
        sku_df2 = sku_data()
        sku_df2["location_id"] = "4"
        sqlite.materialize(data=sku_df2, schema="main", name="sku", overwrite=True)

        result = sqlite.query("SELECT * FROM main.sku")

    assert result.equals(sku_df2)
