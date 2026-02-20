import polars as pl
from adapta.dataclass_validation import AbstractDataClass, Field, Checks
from adapta.dataclass_validation.validation.validation_polars import PolarsValidationClass


def test__validate_ge_value__expected_no_errors():
    """
    Expected no errors since all column values are above 0.0.
    """

    class TestDataClass(AbstractDataClass):
        column_1 = Field(display_name="Column 1", description="Description for column 1.", dtype=str, primary_key=True)
        column_2 = Field(
            display_name="Column 2", description="Description for column 2.", dtype=int, checks=Checks(ge_value=0.0)
        )
        column_3 = Field(
            display_name="Column 3", description="Description for column 3.", dtype=float, checks=Checks(ge_value=0.0)
        )
        column_4 = Field(
            display_name="Column 4",
            description="Description for column 4.",
            dtype=float,
            checks=Checks(ge_value=0.0),
            required=False,
        )

    TEST_SCHEMA = TestDataClass()

    validation_class = PolarsValidationClass(
        data=pl.DataFrame(
            {
                TEST_SCHEMA.column_1: ["value1", "value2"],
                TEST_SCHEMA.column_2: pl.Series([1, 2], dtype=pl.Int64),
                TEST_SCHEMA.column_3: pl.Series([1.0, 2.0], dtype=pl.Float64),
                TEST_SCHEMA.column_4: [-1.0, -2.0],  # non-required column, therefore does not check validation failure
            }
        ),
        schema=TEST_SCHEMA,
        settings=[],
    )

    validation_class._validate_ge_value()

    assert len(validation_class._failed_validations) == 0


def test__validate_ge_value__expected_errors():
    """
    Expected errors since column_2 and column_3 values are below 3.0.
    """

    class TestDataClass(AbstractDataClass):
        column_1 = Field(display_name="Column 1", description="Description for column 1.", dtype=str, primary_key=True)
        column_2 = Field(
            display_name="Column 2", description="Description for column 2.", dtype=int, checks=Checks(ge_value=3.0)
        )
        column_3 = Field(
            display_name="Column 3", description="Description for column 3.", dtype=float, checks=Checks(ge_value=3.0)
        )

    TEST_SCHEMA = TestDataClass()

    validation_class = PolarsValidationClass(
        data=pl.DataFrame(
            {
                TEST_SCHEMA.column_1: ["value1", "value2"],
                TEST_SCHEMA.column_2: pl.Series([1, 2], dtype=pl.Int64),
                TEST_SCHEMA.column_3: pl.Series([1.0, 2.0], dtype=pl.Float64),
            }
        ),
        schema=TEST_SCHEMA,
        settings=[],
    )

    validation_class._validate_ge_value()

    assert validation_class._failed_validations == [
        "Column 'column_2' does not satisfy the greater than or equal to constraint. It should be greater than 3.0.",
        "Column 'column_3' does not satisfy the greater than or equal to constraint. It should be greater than 3.0.",
    ]
