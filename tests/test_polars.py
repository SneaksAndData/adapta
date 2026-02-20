from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import polars
from adapta.storage.models.formatters.polars import get_polars_schema


def test_polars_schema():
    @dataclass
    class Test:
        value_int: int
        value_string: str
        value_bool: bool
        value_datetime: datetime
        value_date: date
        value_float: float

    assert get_polars_schema(Test) == {
        "value_int": polars.Int64,
        "value_string": polars.String,
        "value_bool": polars.Boolean,
        "value_datetime": polars.Datetime,
        "value_date": polars.Date,
        "value_float": polars.Float64,
    }


def test_nested_polars_schema():
    @dataclass
    class TestField:
        id: int
        value: Optional[str] = None

    @dataclass
    class Test:
        date: date
        items: list[TestField]

    assert get_polars_schema(Test) == {
        "date": polars.Date,
        "items": polars.List(polars.Struct({"id": polars.Int64, "value": polars.String})),
    }
