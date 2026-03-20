from dataclasses import dataclass

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from adapta.dataclass_validation import AbstractDataClass, Field, Checks
from adapta.dataclass_validation.validation.validation_polars import PolarsValidationClass


class GeToleranceDataClass(AbstractDataClass):
    primary_column = Field(
        display_name="Primary Column",
        description="Primary key column.",
        dtype=str,
        primary_key=True,
    )
    inventory = Field(
        display_name="Inventory",
        description="Inventory value with default tolerance.",
        dtype=float,
        checks=Checks(ge_value=0.0),
    )


class GeZeroToleranceDataClass(AbstractDataClass):
    primary_column = Field(
        display_name="Primary Column",
        description="Primary key column.",
        dtype=str,
        primary_key=True,
    )
    inventory = Field(
        display_name="Inventory",
        description="Inventory value with zero tolerance.",
        dtype=float,
        checks=Checks(ge_value=0.0, ge_value_tolerance=0.0),
    )


GE_TOLERANCE_SCHEMA = GeToleranceDataClass()
GE_ZERO_TOLERANCE_SCHEMA = GeZeroToleranceDataClass()


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
                schema=GE_TOLERANCE_SCHEMA,
                data=pl.DataFrame(
                    {
                        "primary_column": ["customer_1"],
                        "inventory": pl.Series([-0.00005], dtype=pl.Float64),
                    }
                ),
            ),
            TestOutput(
                expected_data=pl.DataFrame(
                    {
                        "primary_column": ["customer_1"],
                        "inventory": pl.Series([0.0], dtype=pl.Float64),
                    }
                ),
            ),
            id="1) Default tolerance fix value within tolerance to the bound",
        ),
        pytest.param(
            TestInput(
                schema=GE_TOLERANCE_SCHEMA,
                data=pl.DataFrame(
                    {
                        "primary_column": ["customer_1", "customer_2"],
                        "inventory": pl.Series([-0.00005, 5.0], dtype=pl.Float64),
                    }
                ),
            ),
            TestOutput(
                expected_data=pl.DataFrame(
                    {
                        "primary_column": ["customer_1", "customer_2"],
                        "inventory": pl.Series([0.0, 5.0], dtype=pl.Float64),
                    }
                ),
            ),
            id="2) Fix only the value within tolerance while keeping valid values unchanged",
        ),
    ],
)
def test__validate_ge_tolerance__unit_test(inputs: TestInput, expected: TestOutput):
    """
    Test ge_value tolerance clamping behavior:

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
    validation_class._validate_ge_value()

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
                schema=GE_TOLERANCE_SCHEMA,
                data=pl.DataFrame(
                    {
                        "primary_column": ["customer_1"],
                        "inventory": pl.Series([-0.5], dtype=pl.Float64),
                    }
                ),
            ),
            ErrorTestOutput(
                expected_failed_validations=[
                    "Column 'inventory' does not satisfy the greater than or equal to constraint. "
                    "It should be greater than 0.0, but found minimum value -0.5.",
                ],
            ),
            id="1) Default tolerance rejects value far below ge_value",
        ),
        pytest.param(
            ErrorTestInput(
                schema=GE_ZERO_TOLERANCE_SCHEMA,
                data=pl.DataFrame(
                    {
                        "primary_column": ["customer_1"],
                        "inventory": pl.Series([-0.00001], dtype=pl.Float64),
                    }
                ),
            ),
            ErrorTestOutput(
                expected_failed_validations=[
                    "Column 'inventory' does not satisfy the greater than or equal to constraint. "
                    "It should be greater than 0.0, but found minimum value -1e-05.",
                ],
            ),
            id="2) Zero tolerance rejects any value below ge_value",
        ),
    ],
)
def test__validate_ge_tolerance__reports_errors(inputs: ErrorTestInput, expected: ErrorTestOutput):
    """
    Test ge_value tolerance error reporting:

    * 1) Default tolerance rejects value far below ge_value.
    * 2) Zero tolerance rejects any value below ge_value.
    """
    # Arrange
    validation_class = PolarsValidationClass(
        data=inputs.data,
        schema=inputs.schema,
        settings=[],
    )

    # Act
    validation_class._validate_ge_value()

    # Assert
    assert validation_class._failed_validations == expected.expected_failed_validations
