"""
module with polars utility methods
"""

from dataclasses import fields, is_dataclass
from datetime import date, datetime
from typing import Any, get_args, get_origin
import typing

import polars


def get_polars_schema(data_class: Any) -> dict[str, polars.DataType]:
    """Generate a Polars schema from the dataclass fields.

    :param data_class: Dataclass to generate schema from
    :return: dict[str, polars.DataType]: A dictionary mapping field names to their Polars data type representations,
                      where keys are field names (str) and values are Polars data types.
    Example:
        >>> @dataclass
        >>> class MyData:
        >>>     name: str
        >>>     age: int
        >>> get_polars_schema(MyData)
        {'name': pl.String, 'age': pl.Int64}

    """

    if is_dataclass(data_class):
        return {f.name: _map_type(f.type) for f in fields(data_class)}
    raise TypeError(f"input must be dataclass but got {type(data_class)}")


def _map_type(dtype: Any) -> polars.DataType:
    dtype_mapping = {
        str: polars.String,
        int: polars.Int64,
        float: polars.Float64,
        bool: polars.Boolean,
        date: polars.Date,
        datetime: polars.Datetime,
    }
    # Handle nested dataclasses which should be wrapped as struct
    if is_dataclass(dtype):
        return polars.Struct({f.name: _map_type(f.type) for f in fields(dtype)})

    # Handle fields wrapped in Optional
    if get_origin(dtype) == typing.Union:
        return _map_type(get_args(dtype)[0])

    if get_origin(dtype) == list:
        return polars.List(_map_type(get_args(dtype)[0]))

    return dtype_mapping[dtype]
