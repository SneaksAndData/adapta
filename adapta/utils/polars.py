"""
module with polars utility methods
"""

from dataclasses import fields, is_dataclass
from datetime import date, datetime
from typing import Any, get_args, get_origin
import typing
import types

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
        return {f.name: get_polars_type(f.type) for f in fields(data_class)}
    raise TypeError(f"input must be dataclass but got {type(data_class)}")


def get_polars_type(dtype: Any) -> polars.DataType:
    """
    :param dtype: type to convert to polars
    :return: polars data type
    """
    dtype_mapping = {
        str: polars.String,
        int: polars.Int64,
        float: polars.Float64,
        bool: polars.Boolean,
        date: polars.Date,
        datetime: polars.Datetime,
        dict: polars.Struct,
    }
    # Handle nested dataclasses which should be wrapped as struct
    if is_dataclass(dtype):
        return polars.Struct({f.name: get_polars_type(f.type) for f in fields(dtype)})

    # Handle fields wrapped in Optional
    if get_origin(dtype) in (typing.Union, types.UnionType):
        return get_polars_type(get_args(dtype)[0])

    if get_origin(dtype) == list:
        return polars.List(get_polars_type(get_args(dtype)[0]))

    if get_origin(dtype) == dict:
        inner_types = get_args(dtype)

        if len(inner_types) != 2:
            raise ValueError(
                f"expected 2 inner types - one for key and one for value of dict, got "
                f"{len(inner_types)}: {inner_types}"
            )

        return polars.Struct(
            {
                "key": get_polars_type(inner_types[0]),
                "value": get_polars_type(inner_types[1]),
            }
        )

    return dtype_mapping[dtype]
