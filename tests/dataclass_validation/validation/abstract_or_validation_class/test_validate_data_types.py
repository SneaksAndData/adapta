import polars as pl
from adapta.dataclass_validation import AbstractDataClass, Field
from adapta.dataclass_validation.validation.validation_polars import PolarsValidationClass


def test__validate_data_types__expected_no_errors():
    """
    All datatypes are correct, so no errors.
    """

    class TestDataClass(AbstractDataClass):
        column_1 = Field(
            display_name="Column 1", description="Description for column 1.", dtype=str, primary_key=True, coerce=True
        )
        column_2 = Field(
            display_name="Column 2",
            description="Description for column 2.",
            dtype=int,
        )
        column_3 = Field(
            display_name="Column 3",
            description="Description for column 3.",
            dtype=float,
        )
        column_4 = Field(
            display_name="Column 4",
            description="Description for column 4.",
            dtype=bool,
        )

    TEST_SCHEMA = TestDataClass()

    validation_class = PolarsValidationClass(
        data=pl.DataFrame(
            {
                TEST_SCHEMA.column_1: ["value1", "value2"],
                TEST_SCHEMA.column_2: pl.Series([1, 2], dtype=pl.Int64),
                TEST_SCHEMA.column_3: pl.Series([1.0, 2.0], dtype=pl.Float64),
                TEST_SCHEMA.column_4: [True, False],
            }
        ),
        schema=TEST_SCHEMA,
        settings=[],
    )

    validation_class._validate_data_types()

    assert len(validation_class._failed_validations) == 0


def test__validate_data_types__expected_errors():
    """
    All 4 columns have wrong datatypes.
    """

    class TestDataClass(AbstractDataClass):
        column_1 = Field(
            display_name="Column 1", description="Description for column 1.", dtype=str, primary_key=True, coerce=True
        )
        column_2 = Field(
            display_name="Column 2",
            description="Description for column 2.",
            dtype=int,
        )
        column_3 = Field(
            display_name="Column 3",
            description="Description for column 3.",
            dtype=float,
        )
        column_4 = Field(
            display_name="Column 4",
            description="Description for column 4.",
            dtype=bool,
        )

    TEST_SCHEMA = TestDataClass()

    validation_class = PolarsValidationClass(
        data=pl.DataFrame(
            {
                TEST_SCHEMA.column_1: [1],
                TEST_SCHEMA.column_2: ["value"],
                TEST_SCHEMA.column_3: ["value"],
                TEST_SCHEMA.column_4: ["value"],
            }
        ),
        schema=TEST_SCHEMA,
        settings=[],
    )

    validation_class._validate_data_types()

    assert validation_class._failed_validations == [
        "Column 'column_1' has incorrect type. Expected String, got Int64",
        "Column 'column_2' has incorrect type. Expected Int64, got String",
        "Column 'column_3' has incorrect type. Expected Float64, got String",
        "Column 'column_4' has incorrect type. Expected Boolean, got String",
    ]
