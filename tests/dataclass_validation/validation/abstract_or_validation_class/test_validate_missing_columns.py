from dataclasses import dataclass
import polars as pl
import pytest

from adapta.dataclass_validation import AbstractDataClass, Field
from adapta.dataclass_validation.validation.validation_polars import PolarsValidationClass


class ComprehensiveSchema(AbstractDataClass):
    always_required = Field(
        display_name="Always Required", description="Always required field", dtype=str, required=True
    )
    setting_1_required = Field(
        display_name="Setting 1 Required",
        description="Required by setting 1",
        dtype=str,
        required=False,
        required_by_settings=["setting_1"],
    )
    setting_1_2_required = Field(
        display_name="Setting 1 & 2 Required",
        description="Required by setting 1 and 2",
        dtype=str,
        required=False,
        required_by_settings=["setting_1", "setting_2"],
    )
    optional = Field(display_name="Optional", description="Optional field", dtype=str, required=False)


@dataclass
class TestInput:
    dataframe: pl.DataFrame
    settings: list[str]


@dataclass
class TestOutput:
    expected_failed_validations: list[str]


@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        pytest.param(
            TestInput(
                dataframe=pl.DataFrame(
                    {
                        "always_required": ["val"],
                        "setting_1_required": ["val"],
                        "setting_1_2_required": ["val"],
                        "optional": ["val"],
                    }
                ),
                settings=["setting_1", "setting_2"],
            ),
            TestOutput(expected_failed_validations=[]),
            id="All required fields present",
        ),
        pytest.param(
            TestInput(
                dataframe=pl.DataFrame(
                    {
                        "setting_1_required": ["val"],
                        "setting_1_2_required": ["val"],
                        "optional": ["val"],
                    }
                ),
                settings=["setting_1", "setting_2"],
            ),
            TestOutput(expected_failed_validations=["Missing required column: always_required"]),
            id="Missing always required field",
        ),
        pytest.param(
            TestInput(
                dataframe=pl.DataFrame(
                    {
                        "always_required": ["val"],
                        "setting_1_2_required": ["val"],
                        "optional": ["val"],
                    }
                ),
                settings=["setting_1"],
            ),
            TestOutput(
                expected_failed_validations=[
                    "Missing required column: setting_1_required (required by settings: ['setting_1'])"
                ]
            ),
            id="Missing field required by setting 1",
        ),
        pytest.param(
            TestInput(
                dataframe=pl.DataFrame(
                    {
                        "always_required": ["val"],
                        "setting_1_required": ["val"],
                        "optional": ["val"],
                    }
                ),
                settings=["setting_1", "setting_2"],
            ),
            TestOutput(
                expected_failed_validations=[
                    "Missing required column: setting_1_2_required (required by settings: ['setting_1', 'setting_2'])"
                ]
            ),
            id="Missing field required by multiple settings",
        ),
        pytest.param(
            TestInput(
                dataframe=pl.DataFrame(
                    {
                        "always_required": ["val"],
                        "optional": ["val"],
                    }
                ),
                settings=[],
            ),
            TestOutput(expected_failed_validations=[]),
            id="Missing setting required fields but settings not active",
        ),
    ],
)
def test__validate_missing_fields__unit_test(inputs: TestInput, expected: TestOutput):
    """
    Unit test for validating missing fields with various settings configurations.
    """
    schema = ComprehensiveSchema()
    validation_class = PolarsValidationClass(
        data=inputs.dataframe,
        schema=schema,
        settings=inputs.settings,
    )

    validation_class._validate_missing_fields()

    # Sort both lists to ensure order independence in assertion
    assert sorted(validation_class._failed_validations) == sorted(expected.expected_failed_validations)
