from dataclasses import dataclass
import polars as pl
import pytest

from adapta.dataclass_validation import AbstractDataClass, Field
from adapta.dataclass_validation.validation.validation_polars import PolarsValidationClass


@dataclass
class TestInput:
    target_schema: AbstractDataClass
    dataframe: pl.DataFrame
    should_raise: bool = False


@dataclass
class TestOutput:
    expected_dtypes: list[pl.DataType] = None
    expect_failure: bool = False


# Helper to create schema classes dynamically
def create_schema(fields_dict: dict):
    return type("DynamicSchema", (AbstractDataClass,), fields_dict)()


@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        pytest.param(
            TestInput(
                target_schema=create_schema(
                    {
                        "c1": Field(display_name="v1", description="d", dtype=str, coerce=True),
                        "c2": Field(display_name="v2", description="d", dtype=int, coerce=True),
                    }
                ),
                dataframe=pl.DataFrame(
                    {
                        "c1": ["a", "b"],
                        "c2": pl.Series([1, 2], dtype=pl.Int64),
                    }
                ),
            ),
            TestOutput(expected_dtypes=[pl.String, pl.Int64]),
            id="No change when dtypes are already correct",
        ),
        pytest.param(
            TestInput(
                target_schema=create_schema(
                    {
                        "c1": Field(display_name="v1", description="d", dtype=int, coerce=True),
                        "c2": Field(display_name="v2", description="d", dtype=float, coerce=True),
                        "c3": Field(display_name="v3", description="d", dtype=str, coerce=True),
                    }
                ),
                dataframe=pl.DataFrame(
                    {
                        "c1": pl.Series([1, 2], dtype=pl.Int32),
                        "c2": pl.Series([1.0, 2.0], dtype=pl.Float32),
                        "c3": pl.Series([1, 2], dtype=pl.Int64),
                    }
                ),
            ),
            TestOutput(expected_dtypes=[pl.Int64, pl.Float64, pl.String]),
            id="Successful coercion for incorrect upcasts",
        ),
        pytest.param(
            TestInput(
                target_schema=create_schema(
                    {
                        "c1": Field(display_name="v1", description="d", dtype=int, coerce=True),
                    }
                ),
                dataframe=pl.DataFrame(
                    {
                        "c1": ["not_an_int", "error"],
                    }
                ),
                should_raise=False,
            ),
            TestOutput(expect_failure=True),
            id="Log failure instead of raising when should_raise is False",
        ),
    ],
)
def test__coerce_data_types__unit_test(inputs: TestInput, expected: TestOutput):
    """
    Unit test for the validation class coercion logic:

    * No change: Verifies that correct types are left untouched.
    * Expected change: Verifies that 32-bit types are upcast to 64-bit and numeric to string.
    * Failure handling: Verifies that impossible casts (String -> Int) populate failed_validations.
    """
    validation_class = PolarsValidationClass(
        data=inputs.dataframe,
        schema=inputs.target_schema,
        settings=[],
    )

    # Act
    validation_class.coerce_data_types(should_raise=inputs.should_raise)

    # Assert
    if expected.expected_dtypes:
        assert validation_class._data.dtypes == expected.expected_dtypes

    if expected.expect_failure:
        assert len(validation_class._failed_validations) > 0


def test__coerce_data_types__raises():
    """
    Here we test the coercing fails and raises an error since string to int coercing is not possible when should_raise is True.
    1. When should_raise is True, we expect a TypeError to be raised.
    2. When should_raise is False, we expect no error to be raised.
    """

    ### Arrange
    class TestDataClass(AbstractDataClass):
        column_1 = Field(
            display_name="Column 1", description="Description for column 1.", dtype=str, primary_key=True, coerce=True
        )
        column_2 = Field(display_name="Column 2", description="Description for column 2.", dtype=int, coerce=True)

    TEST_SCHEMA = TestDataClass()

    validation_class = PolarsValidationClass(
        data=pl.DataFrame(
            {
                TEST_SCHEMA.column_1: ["value1", "value2"],
                TEST_SCHEMA.column_2: pl.Series(["a", "b"], dtype=pl.String),
            }
        ),
        schema=TEST_SCHEMA,
        settings=[],
    )

    ### Act / Assert
    # Check that it raises when should_raise is True
    with pytest.raises(TypeError) as e:
        validation_class.coerce_data_types(should_raise=True)

    # Check that it does not raise when should_raise is False
    validation_class.coerce_data_types(should_raise=False)
    assert len(validation_class._failed_validations) > 0
