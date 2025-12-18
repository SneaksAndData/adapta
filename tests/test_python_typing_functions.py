import sys
from typing import Any

import pytest

from adapta.utils.python_typing import is_optional


@pytest.mark.parametrize(
    "type_,expected",
    [
        (str | int | None, True),
        (str | None, True),
        (str | None, True),  # Same as Optional[str]
        (str | int | None, True),  # Same as Union[str, int, None], which is an optional type
        (str, False),
        (str | int, False),
        (list[str], False),
        (tuple[int, ...], False),
    ],
)
def test_is_optional(type_: Any, expected: bool):
    """
    Test that the is_optional function correctly identifies optional types.
    """

    assert is_optional(type_) == expected


@pytest.mark.skipif(sys.version_info < (3, 10), reason="Only run this test on Python 3.10+")
def test_is_optional_python310():
    """
    Test that the is_optional function correctly identifies optional types on Python 3.10+.

    str | None is semantically equivalent to Optional[str], but not the same union type
    """

    assert is_optional(str | None)
    assert not is_optional(str | int)
