"""
Tests for the enum field validation.
"""
from dataclasses import dataclass
from enum import Enum

import polars as pl
import pytest

from adapta.dataclass_validation import AbstractDataClass, Field


class MixedEnum(Enum):
    """Enum with mixed-type member values for testing dtype filtering."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    CODE_ONE = 1
    SCORE = 2.0


class PriorityEnum(Enum):
    """Enum with mixed-type member values for priority testing."""

    HIGH = "high"
    LOW = 1
    MEDIUM = 2
    THRESHOLD = 3.5


class EnumDataClass(AbstractDataClass):
    """Schema with fields that have enum constraints."""

    status = Field(
        display_name="Status",
        description="Status field with string allowed values from a mixed enum.",
        dtype=str,
        enum=MixedEnum,
    )
    priority = Field(
        display_name="Priority",
        description="Priority field with int allowed values from a mixed enum.",
        dtype=int,
        enum=PriorityEnum,
    )
    score = Field(
        display_name="Score",
        description="Score field without an enum constraint.",
        dtype=float,
    )
    nullable_status = Field(
        display_name="Nullable Status",
        description="Nullable status field where missing values are allowed.",
        dtype=str,
        enum=MixedEnum,
        allow_missing_values=True,
    )
    strict_status = Field(
        display_name="Strict Status",
        description="Strict status field where missing values are not allowed.",
        dtype=str,
        enum=MixedEnum,
        allow_missing_values=False,
    )


SCHEMA = EnumDataClass()


@dataclass
class InputTest:
    """Input data for the test."""

    data: pl.DataFrame


@dataclass
class OutputTest:
    """Expected output for the test."""

    should_pass: bool
    expected_invalid_column: str = None


@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        pytest.param(
            InputTest(
                data=pl.DataFrame(
                    {
                        SCHEMA.status: ["active", "inactive"],
                        SCHEMA.priority: pl.Series([1, 2], dtype=pl.Int64),
                        SCHEMA.score: [1.0, 2.0],
                        SCHEMA.nullable_status: ["active", "inactive"],
                        SCHEMA.strict_status: ["active", "inactive"],
                    }
                ),
            ),
            OutputTest(should_pass=True),
            id="1) All values within allowed enum sets",
        ),
        pytest.param(
            InputTest(
                data=pl.DataFrame(
                    {
                        SCHEMA.status: ["active", "unknown"],
                        SCHEMA.priority: pl.Series([1, 2], dtype=pl.Int64),
                        SCHEMA.score: [1.0, 2.0],
                        SCHEMA.nullable_status: ["active", "inactive"],
                        SCHEMA.strict_status: ["active", "inactive"],
                    }
                ),
            ),
            OutputTest(should_pass=False, expected_invalid_column="status"),
            id="2) String column contains value not in enum",
        ),
        pytest.param(
            InputTest(
                data=pl.DataFrame(
                    {
                        SCHEMA.status: ["active", "inactive"],
                        SCHEMA.priority: pl.Series([1, 99], dtype=pl.Int64),
                        SCHEMA.score: [1.0, 2.0],
                        SCHEMA.nullable_status: ["active", "inactive"],
                        SCHEMA.strict_status: ["active", "inactive"],
                    }
                ),
            ),
            OutputTest(should_pass=False, expected_invalid_column="priority"),
            id="3) Int column contains value not in dtype-filtered enum",
        ),
        pytest.param(
            InputTest(
                data=pl.DataFrame(
                    {
                        SCHEMA.status: ["active", "inactive"],
                        SCHEMA.priority: pl.Series([1, 2], dtype=pl.Int64),
                        SCHEMA.score: [999.0, 2.0],
                        SCHEMA.nullable_status: ["active", "inactive"],
                        SCHEMA.strict_status: ["active", "inactive"],
                    }
                ),
            ),
            OutputTest(should_pass=True),
            id="4) Column without enum is not validated",
        ),
        pytest.param(
            InputTest(
                data=pl.DataFrame(
                    {
                        SCHEMA.status: ["active", "inactive"],
                        SCHEMA.priority: pl.Series([1, 2], dtype=pl.Int64),
                        SCHEMA.score: [1.0, 2.0],
                        SCHEMA.nullable_status: pl.Series(["active", None], dtype=pl.String),
                        SCHEMA.strict_status: ["active", "inactive"],
                    }
                ),
            ),
            OutputTest(should_pass=True),
            id="5) Nulls in a nullable enum field are accepted",
        ),
        pytest.param(
            InputTest(
                data=pl.DataFrame(
                    {
                        SCHEMA.status: ["active", "inactive"],
                        SCHEMA.priority: pl.Series([1, 2], dtype=pl.Int64),
                        SCHEMA.score: [1.0, 2.0],
                        SCHEMA.nullable_status: ["active", "inactive"],
                        SCHEMA.strict_status: pl.Series(["active", None], dtype=pl.String),
                    }
                ),
            ),
            OutputTest(should_pass=False, expected_invalid_column="strict_status"),
            id="6) Nulls in a non-nullable enum field are treated as invalid",
        ),
    ],
)
def test__validate_enum_members__unit_test(inputs: InputTest, expected: OutputTest):
    """
    Test enum validation logic:

    * 1) Validates that all values pass when within the dtype-filtered enum member values.
    * 2) Detects invalid string values not present in the enum.
    * 3) Filters enum member values by dtype so only int members from a mixed enum are checked.
    * 4) Columns without an enum constraint are unaffected by the validation.
    * 5) Null values in a nullable enum field are excluded from the enum membership check.
    * 6) Null values in a non-nullable enum field are reported as invalid enum members.
    """
    # Act
    validation_response = SCHEMA.validate_and_collect_data(data=inputs.data)

    # Assert
    if expected.should_pass:
        assert (
            len(validation_response.failed_validations) == 0
        ), f"Expected no failures but got: {validation_response.failed_validations}"
    else:
        assert len(validation_response.failed_validations) > 0
        assert any(expected.expected_invalid_column in msg for msg in validation_response.failed_validations)
