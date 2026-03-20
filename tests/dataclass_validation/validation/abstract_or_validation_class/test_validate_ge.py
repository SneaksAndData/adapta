from dataclasses import dataclass
import pytest
import polars as pl
from adapta.dataclass_validation import AbstractDataClass, Field, Checks
from adapta.dataclass_validation.validation.validation_polars import PolarsValidationClass


@dataclass
class TestInput:
    data: pl.DataFrame
    schema: AbstractDataClass


@dataclass
class TestOutput:
    expected_failed_validations: list[str]


@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        pytest.param(
            TestInput(
                data=pl.DataFrame(
                    {
                        "column_1": ["value1", "value2"],
                        "column_2": pl.Series([1, 2], dtype=pl.Int64),
                        "column_3": pl.Series([1.0, 2.0], dtype=pl.Float64),
                    }
                ),
                schema=type(
                    "TestDataClass",
                    (AbstractDataClass,),
                    {
                        "column_1": Field(
                            display_name="Column 1",
                            description="Description for column 1.",
                            dtype=str,
                            primary_key=True,
                        ),
                        "column_2": Field(
                            display_name="Column 2",
                            description="Description for column 2.",
                            dtype=int,
                            checks=Checks(ge_value=3.0),
                        ),
                        "column_3": Field(
                            display_name="Column 3",
                            description="Description for column 3.",
                            dtype=float,
                            checks=Checks(ge_value=3.0),
                        ),
                    },
                )(),
            ),
            TestOutput(
                expected_failed_validations=[
                    "Column 'column_2' does not satisfy the greater than or equal to constraint. It should be greater than 3.0, but found minimum value 1.0.",
                    "Column 'column_3' does not satisfy the greater than or equal to constraint. It should be greater than 3.0, but found minimum value 1.0.",
                ]
            ),
            id="1) Fails ge check and reports actual minimum value",
        ),
        pytest.param(
            TestInput(
                data=pl.DataFrame(
                    {
                        "column_1": ["value1", "value2"],
                        "column_2": pl.Series([4, 5], dtype=pl.Int64),
                        "column_3": pl.Series([4.0, 5.0], dtype=pl.Float64),
                    }
                ),
                schema=type(
                    "TestDataClass",
                    (AbstractDataClass,),
                    {
                        "column_1": Field(
                            display_name="Column 1",
                            description="Description for column 1.",
                            dtype=str,
                            primary_key=True,
                        ),
                        "column_2": Field(
                            display_name="Column 2",
                            description="Description for column 2.",
                            dtype=int,
                            checks=Checks(ge_value=3.0),
                        ),
                        "column_3": Field(
                            display_name="Column 3",
                            description="Description for column 3.",
                            dtype=float,
                            checks=Checks(ge_value=3.0),
                        ),
                    },
                )(),
            ),
            TestOutput(
                expected_failed_validations=[],
            ),
            id="2) Passes ge check with all values above threshold",
        ),
    ],
)
def test__validate_ge_value__unit_test(inputs: TestInput, expected: TestOutput):
    """
    Test ge_value validation:

    * 1) Fails ge check and reports actual minimum value.
    * 2) Passes ge check with all values above threshold.
    """
    # Act
    validation_class = PolarsValidationClass(
        data=inputs.data,
        schema=inputs.schema,
        settings=[],
    )
    validation_class._validate_ge_value()
    # Assert
    assert validation_class._failed_validations == expected.expected_failed_validations
