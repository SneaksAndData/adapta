from dataclasses import dataclass
import pytest
import polars as pl
from polars.testing import assert_frame_equal

from adapta.dataclass_validation.dataclass.dataclass_abstract import AbstractDataClass
from adapta.dataclass_validation.dataclass.dataclass_core import Field


@dataclass
class TestInput:
    dataframe: pl.DataFrame
    schema: AbstractDataClass


@dataclass
class TestOutput:
    expected_dataframe: pl.DataFrame


class SimpleSchema(AbstractDataClass):
    col_1 = Field(display_name="col_1", description="", dtype=int)
    col_2 = Field(display_name="col_2", description="", dtype=str)


@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        pytest.param(
            TestInput(
                dataframe=pl.DataFrame(
                    {
                        "col_1": [1, 2],
                        "col_2": ["a", "b"],
                        "col_3": [True, False],
                    }
                ),
                schema=SimpleSchema(),
            ),
            TestOutput(
                expected_dataframe=pl.DataFrame(
                    {
                        "col_1": [1, 2],
                        "col_2": ["a", "b"],
                    }
                )
            ),
            id="Selects subset of columns",
        ),
    ],
)
def test__coerce_and_select_columns__selects_subset(inputs: TestInput, expected: TestOutput):
    """
    Tests that coerce_and_select_columns selects only the columns defined in the schema.
    """
    result = inputs.schema.coerce_and_select_columns(data=inputs.dataframe)
    assert_frame_equal(result, expected.expected_dataframe, check_dtype=False)


class TestSchema(AbstractDataClass):
    col_1 = Field(display_name="col_1", description="", dtype=int)
    col_2 = Field(display_name="col_2", description="", dtype=str, add_field_if_missing=True, allow_missing_values=True)
    col_3 = Field(display_name="col_3", description="", dtype=bool, required_by_settings=["setting_1", "setting_2"])
    col_4 = Field(display_name="col_4", description="", dtype=str, required=False)


@dataclass
class TestInput:
    dataframe: pl.DataFrame
    add_non_required_fields: bool
    settings: list[str]


@dataclass
class TestExpected:
    expected_dataframe: pl.DataFrame


@pytest.mark.parametrize(
    ("test_input", "test_expected"),
    [
        pytest.param(
            TestInput(
                dataframe=pl.DataFrame(
                    {
                        "col_1": [1, 2],
                        "col_2": ["a", "b"],
                        "col_3": [True, False],
                    }
                ),
                add_non_required_fields=False,
                settings=[],
            ),
            TestExpected(
                expected_dataframe=pl.DataFrame(
                    {
                        "col_1": [1, 2],
                        "col_2": ["a", "b"],
                        "col_3": [True, False],
                    }
                ),
            ),
            id="All required columns present and add_non_required_fields=False - no change.",
        ),
        pytest.param(
            TestInput(
                dataframe=pl.DataFrame(
                    {
                        "col_1": [1, 2],
                        "col_3": [True, False],
                    }
                ),
                add_non_required_fields=False,
                settings=[],
            ),
            TestExpected(
                expected_dataframe=pl.DataFrame(
                    {
                        "col_1": [1, 2],
                        "col_2": pl.Series([None, None], dtype=pl.String),
                        "col_3": [True, False],
                    }
                ),
            ),
            id="Column 2 is missing, but add_field_if_missing is True and hence has been added. add_non_required_fields=False so column 4 is not added.",
        ),
        pytest.param(
            TestInput(
                dataframe=pl.DataFrame(
                    {
                        "col_1": [1, 2],
                        "col_2": ["a", "b"],
                    }
                ),
                add_non_required_fields=True,
                settings=[],
            ),
            TestExpected(
                expected_dataframe=pl.DataFrame(
                    {
                        "col_1": [1, 2],
                        "col_2": ["a", "b"],
                        "col_3": pl.Series([None, None], dtype=pl.Boolean),
                        "col_4": pl.Series([None, None], dtype=pl.String),
                    }
                ),
            ),
            id="Column 3 is missing, but add_missing_settings_columns is True and the settings that require is are inactive and hence has been added."
            "Column 4 is not required and therefore it is added.",
        ),
    ],
)
def test__validate_data__column_adding__functional(test_input: TestInput, test_expected: TestExpected):
    """
    Tests that validate_data correctly adds missing columns.
    Case 1: All required columns present.
    Case 2: Missing fields are added where add_field_if_missing is True.
    Case 3: Missing fields not required by settings are added because add_missing_settings_columns is True.
    """
    # Arrange
    validation_class = TestSchema()

    # Act
    result = validation_class.validate_data(
        data=test_input.dataframe,
        settings=test_input.settings,
        add_non_required_fields=test_input.add_non_required_fields,
    )

    # Assert
    assert_frame_equal(result, test_expected.expected_dataframe, check_column_order=False)


@dataclass
class TestErrorMessage:
    expected_error_message: str


@pytest.mark.parametrize(
    ("test_inputs", "expected_error_message"),
    [
        pytest.param(
            TestInput(
                dataframe=pl.DataFrame(
                    {
                        "col_1": [1, 2],
                        "col_2": ["a", "b"],
                    }
                ),
                add_non_required_fields=True,
                settings=["setting_1"],
            ),
            TestErrorMessage(expected_error_message="Missing required column: col_3"),
            id="Column 3 is missing and required by settings,which results in a validation error.",
        ),
        pytest.param(
            TestInput(
                dataframe=pl.DataFrame(
                    {
                        "col_2": ["a", "b"],
                    }
                ),
                add_non_required_fields=True,
                settings=[],
            ),
            TestErrorMessage(expected_error_message="Missing required column: col_1"),
            id="Column 1 is missing and always required, which results in a validation error.",
        ),
    ],
)
def test__validate_data__missing_columns_raise_value_errors__functional(
    test_inputs: TestInput, expected_error_message: TestErrorMessage
):
    """
    Tests that validate_data raises a ValueError when required columns are missing.
    """
    # Arrange
    validation_class = TestSchema()

    # Act & Assert
    with pytest.raises(ValueError, match=expected_error_message.expected_error_message):
        validation_class.validate_data(
            data=test_inputs.dataframe,
            settings=test_inputs.settings,
            add_non_required_fields=test_inputs.add_non_required_fields,
        )
