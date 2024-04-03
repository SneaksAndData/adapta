"""Common python typing functions. All of these are imported into __init__.py"""
from types import UnionType
from typing import Type, get_origin, Union, get_args


def is_optional(type_: Type) -> bool:
    """
    Checks if a type is Optional.

    :param type_: Type to check.
    :return: True if the type is Optional, False otherwise.
    """
    origin_type = get_origin(type_)
    return (origin_type is Union or origin_type is UnionType) and type(None) in get_args(type_)
