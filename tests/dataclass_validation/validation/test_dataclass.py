import pytest
import polars as pl

from adapta.dataclass_validation import AbstractDataClass, Field, Checks


class TestDataClass(AbstractDataClass):
    column_1 = Field(display_name="Column 1", description="Description for column 1.", dtype=str, primary_key=True)
    column_2 = Field(
        display_name="Column 2",
        description="Description for column 2.",
        dtype=int,
        required=False,
        required_by_settings=["setting_1"],
        coerce=True,
        checks=Checks(ge_value=0.0),
        allow_missing_values=True,
    )
    column_3 = Field(
        display_name="Column 3",
        description="Description for column 3.",
        dtype=float,
        required=False,
        required_by_settings=["setting_2"],
        checks=Checks(
            ge_value=0.0,
            le_value=0.0,
        ),
    )
    column_4 = Field(
        display_name="Column 4",
        description="Description for column 4.",
        required=False,
        dtype=bool,
        add_field_if_missing=True,
        allow_missing_values=True,
    )


class InheritTestDataClass(TestDataClass):
    """
    Example of inheriting from another class
    """

    column_5 = Field(
        display_name="Column 5",
        description="Description for column 5.",
        dtype=str,
    )


class InheritOverwriteTestDataClass(TestDataClass):
    """
    Example of inheriting from another class
    """

    column_4 = Field(
        display_name="Column 4 overrides",
        description="Description for column 4 that overrides the previous definition.",
        dtype=str,
    )


TEST_SCHEMA = TestDataClass()


def test__get_required_fields__expected():
    """
    Test that the get_required_fields method from AbstractDataClass returns the
    expected fields.
    """

    assert list(TEST_SCHEMA.get_required_fields(settings=["setting_1"]).keys()) == ["column_1", "column_2"]


def test__get_primary_keys__expected():
    """
    Test that the get_primary_keys method from AbstractDataClass returns the
    expected fields.
    """

    assert TEST_SCHEMA.get_primary_keys() == ["column_1"]


def test__get_primary_keys__expected_no_keys():
    """
    Test that the get_primary_keys method from AbstractDataClass returns the
    expected fields.
    """

    class TestDataClassNoPrimaryKeys(AbstractDataClass):
        column_1 = Field(
            display_name="Column 1",
            description="Description for column 1.",
            dtype=str,
        )

    assert TestDataClassNoPrimaryKeys().get_primary_keys() == []


def test__get_fields__expected():
    """
    Test that the get_fields method from AbstractDataClass returns the
    expected fields.
    """

    assert list(TEST_SCHEMA.get_fields().keys()) == ["column_1", "column_2", "column_3", "column_4"]


def test__get_allowed_columns_to_add__expected():
    """
    Test that the get_allowed_columns_to_add method from AbstractDataClass returns the
    expected fields.
    """

    assert list(TEST_SCHEMA.get_allowed_fields_to_add().keys()) == ["column_4"]


def test__get_coerce_fields__expected():
    """
    Test that the get_coerce_fields method from AbstractDataClass returns the
    expected fields.
    """

    assert list(TEST_SCHEMA.get_coerce_fields().keys()) == ["column_2"]


def test__get_ge_fields__expected():
    """
    Test that the get_ge_fields method from AbstractDataClass returns the
    expected fields.
    """

    assert list(TEST_SCHEMA.get_ge_value_fields().keys()) == ["column_2", "column_3"]


def test__get_le_fields__expected():
    """
    Test that the get_le_fields method from AbstractDataClass returns the
    expected fields.
    """

    assert list(TEST_SCHEMA.get_le_value_fields().keys()) == ["column_3"]


def test__get_not_allowed_missing_value_fields__expected():
    """
    Test that the get_no_missing_value_fields method from AbstractDataClass returns the expected fields.
    """
    assert list(TEST_SCHEMA.get_not_allowed_missing_value_fields().keys()) == ["column_1", "column_3"]


def test__inherit_fields__expected():
    """
    Test that the class that inherits from another class also gets all the expected parents fields.
    """

    assert sorted(list(InheritTestDataClass().get_fields().keys())) == [
        "column_1",
        "column_2",
        "column_3",
        "column_4",
        "column_5",
    ]


def test__inherit_fields__expected_overwrite_error():
    """
    Test that the class that inherits from another class and overwrites a field is raised as an error.
    """

    with pytest.raises(ValueError):
        InheritOverwriteTestDataClass()


def test__fields_with_checks_and_wrong_types__expected_error():
    """
    Test that the class that has fields with checks and wrong types raises an error.
    """

    with pytest.raises(ValueError):

        class InvalidDataClass(AbstractDataClass):
            column_1 = Field(
                display_name="Column 1", description="Description for column 1.", dtype=str, primary_key=True
            )
            column_2 = Field(
                display_name="Column 2", description="Description for column 2.", dtype=str, checks=Checks(ge_value=0.0)
            )


def test__create_empty_polars_dataframe__expected():
    """
    Test that the create_empty_polars_dataframe method from AbstractDataClass creates
    """
    expected_columns = ["column_1", "column_2", "column_3", "column_4"]
    expected_dtypes = [pl.String, pl.Int64, pl.Float64, pl.Boolean]
    df = TEST_SCHEMA.create_empty_polars_dataframe()
    assert list(df.columns) == expected_columns
    assert list(df.dtypes) == expected_dtypes
