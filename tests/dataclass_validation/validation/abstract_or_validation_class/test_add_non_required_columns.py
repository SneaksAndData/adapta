import polars as pl
from dataclasses import dataclass
from polars.testing import assert_frame_equal
import pytest

from adapta.dataclass_validation import AbstractDataClass, Field
from adapta.dataclass_validation.validation.validation_polars import PolarsValidationClass


class ComprehensiveSchema(AbstractDataClass):
    always_required = Field(
        display_name="Always Required",
        description="Always required field",
        dtype=str,
        required=True,
    )
    setting_1_required = Field(
        display_name="Setting 1 Required",
        description="Required when setting_1 is active",
        dtype=str,
        required=False,
        required_by_settings=["setting_1"],
    )
    optional = Field(
        display_name="Optional",
        description="Always optional field",
        dtype=str,
        required=False,
    )


@dataclass
class TestInput:
    dataframe: pl.DataFrame
    settings: list[str]


@dataclass
class TestOutput:
    expected_dataframe: pl.DataFrame


@pytest.mark.parametrize(
    ("test_inputs", "test_expected"),
    [
        # --- Case 1: Adds optional + inactive setting-required field
        pytest.param(
            TestInput(dataframe=pl.DataFrame({"always_required": ["value"]}), settings=[]),
            TestOutput(
                expected_dataframe=pl.DataFrame(
                    {
                        "always_required": ["value"],
                        "setting_1_required": pl.Series([None], dtype=pl.String),
                        "optional": pl.Series([None], dtype=pl.String),
                    }
                ),
            ),
        ),
        # --- Case 2: Setting is active → do NOT add setting_1_required
        pytest.param(
            TestInput(dataframe=pl.DataFrame({"always_required": ["value"]}), settings=["setting_1"]),
            TestOutput(
                expected_dataframe=pl.DataFrame(
                    {
                        "always_required": ["value"],
                        "optional": pl.Series([None], dtype=pl.String),
                    }
                ),
            ),
        ),
        # --- Case 3: No change because all fields already exist
        pytest.param(
            TestInput(
                dataframe=pl.DataFrame(
                    {
                        "always_required": ["value"],
                        "setting_1_required": ["value"],
                        "optional": ["value"],
                    }
                ),
                settings=[],
            ),
            TestOutput(
                expected_dataframe=pl.DataFrame(
                    {
                        "always_required": ["value"],
                        "setting_1_required": ["value"],
                        "optional": ["value"],
                    }
                ),
            ),
        ),
    ],
)
def test__add_non_required_fields__parametrized(test_inputs, test_expected):
    schema = ComprehensiveSchema()
    validation_class = PolarsValidationClass(
        data=test_inputs.dataframe,
        schema=schema,
        settings=test_inputs.settings,
    )

    validation_class._add_non_required_fields()

    assert_frame_equal(validation_class._data, test_expected.expected_dataframe, check_column_order=False)
