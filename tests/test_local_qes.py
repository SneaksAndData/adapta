from io import BytesIO

import polars as pl
import pytest
from adapta.storage.models import LocalPath
from adapta.storage.models.enum import QueryEnabledStoreOptions
from adapta.storage.models.filter_expression import FilterExpression, FilterField
from adapta.storage.query_enabled_store import LocalQueryEnabledStore, LocalSettings, LocalCredential
from adapta.utils.metaframe import MetaFrameOptions

# Create a test dataset
data = pl.DataFrame(
    {
        "list_of_strings": [["apple", "banana"], ["carrot", "date"], ["elephant", "fig"]],
        "list_of_integers": [[1, 2], [3, 4], [5, 6]],
        "struct_column": [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}, {"name": "Doe", "age": 40}],
        "string_column": ["Hello", "World", "Polars"],
        "float_column": [1.1, 2.2, 3.3],
        "integer_column": [10, 20, 30],
    }
)


@pytest.mark.parametrize(
    "polars_filters, qes_filters",
    [
        (
            pl.col("list_of_strings") == ["carrot", "date"],
            FilterField("list_of_strings").isin([["carrot", "date"]]),
        ),
        (pl.col("list_of_strings") == ["carrot", "date"], FilterField("list_of_strings") == ["carrot", "date"]),
        (
            (pl.col("list_of_strings") == ["carrot", "date"]) | (pl.col("list_of_strings") == ["apple", "banana"]),
            FilterField("list_of_strings").isin([["carrot", "date"], ["apple", "banana"]]),
        ),
        (
            (pl.col("list_of_integers") == [5, 6]) | (pl.col("list_of_integers") == [1, 2]),
            FilterField("list_of_integers").isin([[5, 6], [1, 2]]),
        ),
        (
            (pl.col("list_of_integers") == [5, 6]) | (pl.col("list_of_integers") == [1, 2]),
            (FilterField("list_of_integers") == [5, 6]) | (FilterField("list_of_integers") == [1, 2]),
        ),
        (
            pl.col("struct_column") == {"name": "John", "age": 30},
            FilterField("struct_column") == {"name": "John", "age": 30},
        ),
        (
            (pl.col("struct_column") == {"name": "John", "age": 30})
            & (pl.col("list_of_strings") == ["apple", "banana"]),
            (FilterField("struct_column") == {"name": "John", "age": 30})
            & (FilterField("list_of_strings").isin([["apple", "banana"]])),
        ),
        (
            pl.col("struct_column").is_in([{"name": "John", "age": 30}, {"name": "Doe", "age": 40}]),
            FilterField("struct_column").isin([{"name": "John", "age": 30}, {"name": "Doe", "age": 40}]),
        ),
        (
            (pl.col("float_column") > 1) & (pl.col("float_column") < 2),
            (FilterField("float_column") > 1) & (FilterField("float_column") < 2),
        ),
        (
            (pl.col("integer_column") > 10) & (pl.col("integer_column") <= 20),
            (FilterField("integer_column") > 10) & (FilterField("integer_column") <= 20),
        ),
        (
            pl.col("integer_column").is_in([10, 20]),
            FilterField("integer_column").isin([10, 20]),
        ),
        (
            pl.col("string_column").is_in(["Polars"]),
            FilterField("string_column").isin(["Polars"]),
        ),
        (None, None),
    ],
)
def test_local_qes_read(polars_filters: pl.Expr, qes_filters: FilterExpression):
    store = LocalQueryEnabledStore(settings=LocalSettings(), credentials=LocalCredential())

    bytes_io = BytesIO()
    data.write_parquet(bytes_io)
    bytes_io.seek(0)

    polars_data = data.filter(polars_filters) if polars_filters is not None else data
    qes_data = (
        store.open(LocalPath(path=bytes_io))
        .select(*data.columns)
        .filter(qes_filters)
        .add_options(
            option_key=QueryEnabledStoreOptions.CONCAT_OPTIONS, option_value=[MetaFrameOptions(how="vertical")]
        )
        .read()
        .to_polars()
    )

    assert polars_data.equals(qes_data)
