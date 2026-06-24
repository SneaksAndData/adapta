from dataclasses import dataclass

import pytest

from adapta.dataclass_validation import Checks, Field


@dataclass
class InputTest:
    field_instance: Field


@dataclass
class OutputTest:
    expected_required: bool
    expected_settings: list[str]


@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        pytest.param(
            InputTest(
                field_instance=Field(
                    display_name="Values",
                    description="List of integer values.",
                    dtype=list[int],
                    checks=Checks(ge_value=0.0),
                )
            ),
            OutputTest(expected_required=True, expected_settings=[]),
            id="1) Allows ge checks on list of integers",
        ),
        pytest.param(
            InputTest(
                field_instance=Field(
                    display_name="Values",
                    description="List of float values.",
                    dtype=list[float],
                    checks=Checks(le_value=10.0),
                )
            ),
            OutputTest(expected_required=True, expected_settings=[]),
            id="2) Allows le checks on list of floats",
        ),
    ],
)
def test__field_initialization__numeric_list_checks(inputs: InputTest, expected: OutputTest):
    """
    Test Field initialization for numeric list checks:

    * 1) Allows greater-than-or-equal checks on list[int] fields.
    * 2) Allows less-than-or-equal checks on list[float] fields.
    """

    # Arrange
    field = inputs.field_instance

    # Assert
    assert field.required == expected.expected_required
    assert field.required_by_settings == expected.expected_settings
