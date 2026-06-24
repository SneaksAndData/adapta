import polars as pl
from adapta.dataclass_validation import AbstractDataClass, Field
from adapta.dataclass_validation.validation.validation_polars import PolarsValidationClass


def test__add_missing_fields__expected_no_change():
    """
    No fields should be added if they are missing.
    """

    class TestDataClass(AbstractDataClass):
        column_1 = Field(
            display_name="Column 1",
            description="Description for column 1.",
            dtype=str,
            primary_key=True,
            required=True,
        )

    validation_class = PolarsValidationClass(
        data=pl.DataFrame(),
        schema=TestDataClass(),
        settings=[],
    )

    validation_class._add_missing_fields()

    assert len(validation_class._data.columns) == 0


def test__add_missing_fields__expected_added_column():
    """
    Column 1 should be added to the dataframe, since it's missing before the validation and the setting
    "add_field_if_missing" is enabled.
    """

    class TestDataClass(AbstractDataClass):
        column_1 = Field(
            display_name="Column 1",
            description="Description for column 1.",
            dtype=str,
            required=True,
            add_field_if_missing=True,
            allow_missing_values=True,
        )

    validation_class = PolarsValidationClass(
        data=pl.DataFrame(),
        schema=TestDataClass(),
        settings=[],
    )

    validation_class._add_missing_fields()

    assert validation_class._data.columns == ["column_1"]
