from dataclasses import dataclass

import polars as pl
import pytest
from polars.testing import assert_frame_equal

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
    expected_data: pl.DataFrame


@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        pytest.param(
            InputTest(
                schema=GE_FLOAT_LIST_SCHEMA,
                data=pl.DataFrame(
                    {
                        GE_FLOAT_LIST_SCHEMA.record_id: ["customer_1", "customer_2"],
                        GE_FLOAT_LIST_SCHEMA.values: [[-0.00005, 1.5], [2.0, 3.0]],
                    }
                ),
            ),
            OutputTest(
                expected_data=pl.DataFrame(
                    {
                        GE_FLOAT_LIST_SCHEMA.record_id: ["customer_1", "customer_2"],
                        GE_FLOAT_LIST_SCHEMA.values: [[0.0, 1.5], [2.0, 3.0]],
                    }
                ),
            ),
            id="1) Clamps float list values within ge tolerance",
        ),
        pytest.param(
            InputTest(
                schema=GE_INT_LIST_SCHEMA,
                data=pl.DataFrame(
                    {
                        GE_INT_LIST_SCHEMA.record_id: ["customer_1", "customer_2"],
                        GE_INT_LIST_SCHEMA.values: [[-1, 5], [2, 3]],
                    }
                ),
            ),
            OutputTest(
                expected_data=pl.DataFrame(
                    {
                        GE_INT_LIST_SCHEMA.record_id: ["customer_1", "customer_2"],
                        GE_INT_LIST_SCHEMA.values: [[0, 5], [2, 3]],
                    }
                ),
            ),
            id="2) Clamps integer list values within ge tolerance",
        ),
    ],
)
def test__validate_ge_value__list_columns(inputs: InputTest, expected: OutputTest):
    """
    Test greater-than-or-equal validation for list columns:

    * 1) Clamps float list values within tolerance to the lower bound.
    * 2) Clamps integer list values within tolerance to the lower bound.
    """

    # Arrange
    validation_class = PolarsValidationClass(data=inputs.data, schema=inputs.schema, settings=[])

    # Act
    validation_class._validate_ge_value()

    # Assert
    assert_frame_equal(validation_class._data, expected.expected_data)


@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        pytest.param(
            InputTest(
                schema=LE_FLOAT_LIST_SCHEMA,
                data=pl.DataFrame(
                    {
                        LE_FLOAT_LIST_SCHEMA.record_id: ["customer_1", "customer_2"],
                        LE_FLOAT_LIST_SCHEMA.values: [[100.00005, 50.0], [25.0, 75.0]],
                    }
                ),
            ),
            OutputTest(
                expected_data=pl.DataFrame(
                    {
                        LE_FLOAT_LIST_SCHEMA.record_id: ["customer_1", "customer_2"],
                        LE_FLOAT_LIST_SCHEMA.values: [[100.0, 50.0], [25.0, 75.0]],
                    }
                ),
            ),
            id="1) Clamps float list values within le tolerance",
        ),
        pytest.param(
            InputTest(
                schema=LE_INT_LIST_SCHEMA,
                data=pl.DataFrame(
                    {
                        LE_INT_LIST_SCHEMA.record_id: ["customer_1", "customer_2"],
                        LE_INT_LIST_SCHEMA.values: [[101, 50], [25, 75]],
                    }
                ),
            ),
            OutputTest(
                expected_data=pl.DataFrame(
                    {
                        LE_INT_LIST_SCHEMA.record_id: ["customer_1", "customer_2"],
                        LE_INT_LIST_SCHEMA.values: [[100, 50], [25, 75]],
                    }
                ),
            ),
            id="2) Clamps integer list values within le tolerance",
        ),
    ],
)
def test__validate_le_value__list_columns(inputs: InputTest, expected: OutputTest):
    """
    Test less-than-or-equal validation for list columns:

    * 1) Clamps float list values within tolerance to the upper bound.
    * 2) Clamps integer list values within tolerance to the upper bound.
    """

    # Arrange
    validation_class = PolarsValidationClass(data=inputs.data, schema=inputs.schema, settings=[])

    # Act
    validation_class._validate_le_value()

    # Assert
    assert_frame_equal(validation_class._data, expected.expected_data)
