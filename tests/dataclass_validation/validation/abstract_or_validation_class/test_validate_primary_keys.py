import polars as pl
from adapta.dataclass_validation import AbstractDataClass, Field
from adapta.dataclass_validation.validation.validation_polars import PolarsValidationClass


def test__validate_primary_keys__expected_unique_primary_keys():
    """
    Primary keys are unique, hence we have no errors.
    """

    class TestDataClass(AbstractDataClass):
        column_1 = Field(
            display_name="Column 1",
            description="Description for column 1.",
            dtype=str,
            primary_key=True,
            required=True,
        )
        column_2 = Field(
            display_name="Column 2",
            description="Description for column 2.",
            dtype=str,
            primary_key=True,
            required=True,
        )

    TEST_SCHEMA = TestDataClass()

    validation_class = PolarsValidationClass(
        data=pl.DataFrame({TEST_SCHEMA.column_1: ["value1", "value2"], TEST_SCHEMA.column_2: ["value2", "value2"]}),
        schema=TEST_SCHEMA,
        settings=[],
    )

    validation_class._validate_primary_keys()

    assert len(validation_class._failed_validations) == 0


def test__validate_primary_keys__expected_not_unique_primary_keys():
    """
    Primary keys are not unique, hence we have an error.
    """

    class TestDataClass(AbstractDataClass):
        column_1 = Field(
            display_name="Column 1",
            description="Description for column 1.",
            dtype=str,
            primary_key=True,
            required=True,
        )
        column_2 = Field(
            display_name="Column 2",
            description="Description for column 2.",
            dtype=str,
            primary_key=True,
            required=True,
        )

    TEST_SCHEMA = TestDataClass()

    validation_class = PolarsValidationClass(
        data=pl.DataFrame(
            {
                TEST_SCHEMA.column_1: ["value1", "value1"],
                TEST_SCHEMA.column_2: ["value2", "value2"],
            }
        ),
        schema=TEST_SCHEMA,
        settings=[],
    )

    validation_class._validate_primary_keys()

    assert validation_class._failed_validations == [
        "Duplicated primary key(s) found. Please ensure primary key(s) are unique. This is the provided primary key(s): ['column_1', 'column_2']"
    ]


def test__validate_primary_keys__no_primary_keys_set():
    """
    Primary keys are not set, hence _validate_primary_keys should do nothing.
    """

    class TestDataClass(AbstractDataClass):
        column_1 = Field(
            display_name="Column 1",
            description="Description for column 1.",
            dtype=str,
            required=True,
        )
        column_2 = Field(
            display_name="Column 2",
            description="Description for column 2.",
            dtype=str,
            required=True,
        )

    TEST_SCHEMA = TestDataClass()

    validation_class = PolarsValidationClass(
        data=pl.DataFrame(
            {
                TEST_SCHEMA.column_1: ["value1", "value1"],
                TEST_SCHEMA.column_2: ["value2", "value2"],
            }
        ),
        schema=TEST_SCHEMA,
        settings=[],
    )

    validation_class._validate_primary_keys()

    assert len(validation_class._failed_validations) == 0
