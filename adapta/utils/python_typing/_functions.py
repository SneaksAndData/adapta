"""Common python typing functions. All of these are imported into __init__.py"""
from typing import get_origin, Union, get_args
from types import UnionType

ArgumentType = type

ArgumentType = Union[UnionType, type]


def is_optional(type_: ArgumentType) -> bool:
    """
    Checks if a type is Optional.

    :param type_: Type to check.
    :return: True if the type is Optional, False otherwise.
    """
    origin_type = get_origin(type_)

    return (origin_type is UnionType or origin_type is Union) and type(None) in get_args(type_)
