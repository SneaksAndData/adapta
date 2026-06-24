import polars as pl
import pytest
from adapta.dataclass_validation import AbstractDataClass, Field, Checks
from adapta.dataclass_validation.validation.validation_polars import PolarsValidationClass


class TestDataClass(AbstractDataClass):
    column_1 = Field(display_name="Column 1", description="Description for column 1.", dtype=str, primary_key=True)
    column_2 = Field(
        display_name="Column 2",
        description="Description for column 2.",
        dtype=int,
        allow_missing_values=True,
    )
    column_3 = Field(
        display_name="Column 3",
        description="Description for column 3.",
        dtype=float,
        allow_missing_values=False,
    )
    column_4 = Field(
        display_name="Column 4",
        description="Description for column 4.",
        dtype=float,
        allow_missing_values=False,
        required=False,
    )


TEST_SCHEMA = TestDataClass()


@pytest.mark.parametrize(
    "input_data",
    [
        pytest.param(
            pl.DataFrame(
                {
                    TEST_SCHEMA.column_1: ["value1", "value2"],
                    TEST_SCHEMA.column_2: pl.Series([1, 2], dtype=pl.Int64),
                    TEST_SCHEMA.column_3: pl.Series([1.0, 2.0], dtype=pl.Float64),
                    TEST_SCHEMA.column_4: [1.0, 2.0],
                }
            ),
            id="No missing values.",
        ),
        pytest.param(
            pl.DataFrame(
                {
                    TEST_SCHEMA.column_1: ["value1", "value2"],
                    TEST_SCHEMA.column_2: pl.Series([1, None], dtype=pl.Int64),
                    TEST_SCHEMA.column_3: pl.Series([1.0, 2.0], dtype=pl.Float64),
                    TEST_SCHEMA.column_4: [1.0, 2.0],
                }
            ),
            id="Missing values in column_2, where missing values are allowed.",
        ),
        pytest.param(
            pl.DataFrame(
                {
                    TEST_SCHEMA.column_1: ["value1", "value2"],
                    TEST_SCHEMA.column_2: pl.Series([1, 2], dtype=pl.Int64),
                    TEST_SCHEMA.column_3: pl.Series([1.0, 2.0], dtype=pl.Float64),
                    TEST_SCHEMA.column_4: [None, None],
                }
            ),
            id="Missing values in column_4, which is not required.",
        ),
    ],
)
def test__validate_value_not_missing__expected_no_errors(input_data):
    """
    Expected no errors as no values are missing.
    """

    validation_class = PolarsValidationClass(
        data=input_data,
        schema=TEST_SCHEMA,
        settings=[],
    )

    validation_class._validate_value_not_missing()

    assert len(validation_class._failed_validations) == 0


@pytest.mark.parametrize(
    "input_data",
    [
        pytest.param(
            pl.DataFrame(
                {
                    TEST_SCHEMA.column_1: ["value1", "value2"],
                    TEST_SCHEMA.column_2: pl.Series([1, 2], dtype=pl.Int64),
                    TEST_SCHEMA.column_3: pl.Series([1.0, None], dtype=pl.Float64),
                }
            ),
            id="One value missing in column_3.",
        ),
        pytest.param(
            pl.DataFrame(
                {
                    TEST_SCHEMA.column_1: ["value1", "value2"],
                    TEST_SCHEMA.column_2: pl.Series([1, 2], dtype=pl.Int64),
                    TEST_SCHEMA.column_3: pl.Series([None, None], dtype=pl.Float64),
                }
            ),
            id="All values missing from column_3.",
        ),
    ],
)
def test___validate_value_not_missing__expected_errors(input_data):
    """
    Tests that the validation throws an error both when all values are missing and when only some values are missing
    from a column that doesn't allow missing values.
    """

    validation_class = PolarsValidationClass(
        data=input_data,
        schema=TEST_SCHEMA,
        settings=[],
    )

    validation_class._validate_value_not_missing()

    assert validation_class._failed_validations == [
        "Column 'column_3' does not allow missing values but contains missing values.",
    ]
