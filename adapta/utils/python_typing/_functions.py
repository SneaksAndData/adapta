"""Common python typing functions. All of these are imported into __init__.py"""
import sys
from typing import Type, get_origin, Union, get_args

TypeHint = Type
if sys.version_info >= (3, 10):
    from types import UnionType
    TypeHint = Union[UnionType, Type]  # pylint: disable=invalid-name


def is_optional(type_: TypeHint) -> bool:
    """
    Checks if a type is Optional.

    :param type_: Type to check.
    :return: True if the type is Optional, False otherwise.
    """
    origin_type = get_origin(type_)

    if sys.version_info >= (3, 10):
        return (origin_type is UnionType or origin_type is Union) and type(None) in get_args(type_)

    return origin_type is Union and type(None) in get_args(type_)
