from dataclasses import dataclass

import polars as pl
import pytest

from adapta.dataclass_validation import AbstractDataClass, Checks, Field
from adapta.dataclass_validation.validation.validation_polars import PolarsValidationClass


class GeFloatListDataClass(AbstractDataClass):
    record_id = Field(display_name="Record ID", description="Primary key column.", dtype=str, primary_key=True)
    values = Field(
        display_name="Values",
        description="Float values validated with a greater-than-or-equal check.",
        dtype=list[float],
        checks=Checks(ge_value=0.0),
    )


class GeIntListDataClass(AbstractDataClass):
    record_id = Field(display_name="Record ID", description="Primary key column.", dtype=str, primary_key=True)
    values = Field(
        display_name="Values",
        description="Integer values validated with a greater-than-or-equal check.",
        dtype=list[int],
        checks=Checks(ge_value=0.0, ge_value_tolerance=1.0),
    )


class LeFloatListDataClass(AbstractDataClass):
    record_id = Field(display_name="Record ID", description="Primary key column.", dtype=str, primary_key=True)
    values = Field(
        display_name="Values",
        description="Float values validated with a less-than-or-equal check.",
        dtype=list[float],
        checks=Checks(le_value=100.0),
    )


class LeIntListDataClass(AbstractDataClass):
    record_id = Field(display_name="Record ID", description="Primary key column.", dtype=str, primary_key=True)
    values = Field(
        display_name="Values",
        description="Integer values validated with a less-than-or-equal check.",
        dtype=list[int],
        checks=Checks(le_value=100.0, le_value_tolerance=1.0),
    )


GE_FLOAT_LIST_SCHEMA = GeFloatListDataClass()
GE_INT_LIST_SCHEMA = GeIntListDataClass()
LE_FLOAT_LIST_SCHEMA = LeFloatListDataClass()
LE_INT_LIST_SCHEMA = LeIntListDataClass()


@dataclass
class InputTest:
    schema: AbstractDataClass
    data: pl.DataFrame


@dataclass
class OutputTest:
    expected_failed_validations: list[str]


@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        pytest.param(
            InputTest(
                schema=GE_FLOAT_LIST_SCHEMA,
                data=pl.DataFrame(
                    {
                        GE_FLOAT_LIST_SCHEMA.record_id: ["customer_1"],
                        GE_FLOAT_LIST_SCHEMA.values: [[-0.5, 1.0]],
                    }
                ),
            ),
            OutputTest(
                expected_failed_validations=[
                    "Column 'values' does not satisfy the greater than or equal to constraint. "
                    "It should be greater than 0.0, but found minimum value -0.5.",
                ],
            ),
            id="1) Reports ge error for float list values below tolerance",
        ),
        pytest.param(
            InputTest(
                schema=GE_INT_LIST_SCHEMA,
                data=pl.DataFrame(
                    {
                        GE_INT_LIST_SCHEMA.record_id: ["customer_1"],
                        GE_INT_LIST_SCHEMA.values: [[-2, 1]],
                    }
                ),
            ),
            OutputTest(
                expected_failed_validations=[
                    "Column 'values' does not satisfy the greater than or equal to constraint. "
                    "It should be greater than 0.0, but found minimum value -2.",
                ],
            ),
            id="2) Reports ge error for integer list values below tolerance",
        ),
    ],
)
def test__validate_ge_value__list_column_errors(inputs: InputTest, expected: OutputTest):
    """
    Test greater-than-or-equal validation errors for list columns:

    * 1) Reports a ge error for float list values that fall outside tolerance.
    * 2) Reports a ge error for integer list values that fall outside tolerance.
    """

    # Arrange
    validation_class = PolarsValidationClass(data=inputs.data, schema=inputs.schema, settings=[])

    # Act
    validation_class._validate_ge_value()

    # Assert
    assert validation_class._failed_validations == expected.expected_failed_validations


@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        pytest.param(
            InputTest(
                schema=LE_FLOAT_LIST_SCHEMA,
                data=pl.DataFrame(
                    {
                        LE_FLOAT_LIST_SCHEMA.record_id: ["customer_1"],
                        LE_FLOAT_LIST_SCHEMA.values: [[100.5, 80.0]],
                    }
                ),
            ),
            OutputTest(
                expected_failed_validations=[
                    "Column 'values' does not satisfy the less than or equal to constraint. "
                    "It should be less than 100.0, but found maximum value 100.5.",
                ],
            ),
            id="1) Reports le error for float list values above tolerance",
        ),
        pytest.param(
            InputTest(
                schema=LE_INT_LIST_SCHEMA,
                data=pl.DataFrame(
                    {
                        LE_INT_LIST_SCHEMA.record_id: ["customer_1"],
                        LE_INT_LIST_SCHEMA.values: [[102, 80]],
                    }
                ),
            ),
            OutputTest(
                expected_failed_validations=[
                    "Column 'values' does not satisfy the less than or equal to constraint. "
                    "It should be less than 100.0, but found maximum value 102.",
                ],
            ),
            id="2) Reports le error for integer list values above tolerance",
        ),
    ],
)
def test__validate_le_value__list_column_errors(inputs: InputTest, expected: OutputTest):
    """
    Test less-than-or-equal validation errors for list columns:

    * 1) Reports a le error for float list values that fall outside tolerance.
    * 2) Reports a le error for integer list values that fall outside tolerance.
    """

    # Arrange
    validation_class = PolarsValidationClass(data=inputs.data, schema=inputs.schema, settings=[])

    # Act
    validation_class._validate_le_value()

    # Assert
    assert validation_class._failed_validations == expected.expected_failed_validations
