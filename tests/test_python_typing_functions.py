from typing import Optional, Union, List, Tuple, Any

import pytest

from adapta.utils.python_typing import is_optional


@pytest.mark.parametrize(
    "test_type,expected",
    [
        (is_optional(Optional[Union[str, int]]), True),
        (is_optional(Optional[str]), True),
        (is_optional(Union[str, None]), True),  # Same as Optional[str]
        (is_optional(Union[str, Optional[int]]), True),  # Same as Union[str, int, None], which is an optional type
        (is_optional(str), False),
        (is_optional(Union[str, int]), False),
        (is_optional(List[str]), False),
        (is_optional(Tuple[int, ...]), False),
    ],
)
def test_is_optional(test_type: Any, expected: bool):
    """
    Test that the is_optional function correctly identifies optional types.
    """

    assert test_type == expected
