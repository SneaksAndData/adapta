from dataclasses import dataclass

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from adapta.dataclass_validation import AbstractDataClass, Field, Checks
from adapta.dataclass_validation.validation.validation_polars import PolarsValidationClass


class LeToleranceDataClass(AbstractDataClass):
    primary_column = Field(
        display_name="Primary Column",
        description="Primary key column.",
        dtype=str,
        primary_key=True,
    )
    capacity = Field(
        display_name="Capacity",
        description="Capacity value with default tolerance.",
        dtype=float,
        checks=Checks(le_value=100.0),
    )


class LeZeroToleranceDataClass(AbstractDataClass):
    primary_column = Field(
        display_name="Primary Column",
        description="Primary key column.",
        dtype=str,
        primary_key=True,
    )
    capacity = Field(
        display_name="Capacity",
        description="Capacity value with zero tolerance.",
        dtype=float,
        checks=Checks(le_value=100.0, le_value_tolerance=0.0),
    )


LE_TOLERANCE_SCHEMA = LeToleranceDataClass()
LE_ZERO_TOLERANCE_SCHEMA = LeZeroToleranceDataClass()


@dataclass
class TestInput:
    schema: AbstractDataClass
    data: pl.DataFrame


@dataclass
class TestOutput:
    expected_data: pl.DataFrame


@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        pytest.param(
            TestInput(
                schema=LE_TOLERANCE_SCHEMA,
                data=pl.DataFrame(
                    {
                        "primary_column": ["customer_1"],
                        "capacity": pl.Series([100.00005], dtype=pl.Float64),
                    }
                ),
            ),
            TestOutput(
                expected_data=pl.DataFrame(
                    {
                        "primary_column": ["customer_1"],
                        "capacity": pl.Series([100.0], dtype=pl.Float64),
                    }
                ),
            ),
            id="1) Default tolerance fixes value within tolerance to the bound",
        ),
        pytest.param(
            TestInput(
                schema=LE_TOLERANCE_SCHEMA,
                data=pl.DataFrame(
                    {
                        "primary_column": ["customer_1", "customer_2"],
                        "capacity": pl.Series([100.00005, 50.0], dtype=pl.Float64),
                    }
                ),
            ),
            TestOutput(
                expected_data=pl.DataFrame(
                    {
                        "primary_column": ["customer_1", "customer_2"],
                        "capacity": pl.Series([100.0, 50.0], dtype=pl.Float64),
                    }
                ),
            ),
            id="2) Fixes only the value within tolerance while keeping valid values unchanged",
        ),
    ],
)
def test__validate_le_tolerance__unit_test(inputs: TestInput, expected: TestOutput):
    """
    Test le_value tolerance clamping behavior:

    * 1) Default tolerance clamps value within tolerance to the bound.
    * 2) Clamps only the value within tolerance while keeping valid values unchanged.
    """
    # Arrange
    validation_class = PolarsValidationClass(
        data=inputs.data,
        schema=inputs.schema,
        settings=[],
    )

    # Act
    validation_class._validate_le_value()

    # Assert
    assert_frame_equal(validation_class._data, expected.expected_data)


@dataclass
class ErrorTestInput:
    schema: AbstractDataClass
    data: pl.DataFrame


@dataclass
class ErrorTestOutput:
    expected_failed_validations: list[str]


@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        pytest.param(
            ErrorTestInput(
                schema=LE_TOLERANCE_SCHEMA,
                data=pl.DataFrame(
                    {
                        "primary_column": ["customer_1"],
                        "capacity": pl.Series([100.5], dtype=pl.Float64),
                    }
                ),
            ),
            ErrorTestOutput(
                expected_failed_validations=[
                    "Column 'capacity' does not satisfy the less than or equal to constraint. "
                    "It should be less than 100.0.",
                ],
            ),
            id="1) Default tolerance rejects value far above le_value",
        ),
        pytest.param(
            ErrorTestInput(
                schema=LE_ZERO_TOLERANCE_SCHEMA,
                data=pl.DataFrame(
                    {
                        "primary_column": ["customer_1"],
                        "capacity": pl.Series([100.00001], dtype=pl.Float64),
                    }
                ),
            ),
            ErrorTestOutput(
                expected_failed_validations=[
                    "Column 'capacity' does not satisfy the less than or equal to constraint. "
                    "It should be less than 100.0.",
                ],
            ),
            id="2) Zero tolerance rejects any value above le_value",
        ),
    ],
)
def test__validate_le_tolerance__reports_errors(inputs: ErrorTestInput, expected: ErrorTestOutput):
    """
    Test le_value tolerance error reporting:

    * 1) Default tolerance rejects value far above le_value.
    * 2) Zero tolerance rejects any value above le_value.
    """
    # Arrange
    validation_class = PolarsValidationClass(
        data=inputs.data,
        schema=inputs.schema,
        settings=[],
    )

    # Act
    validation_class._validate_le_value()

    # Assert
    assert validation_class._failed_validations == expected.expected_failed_validations
