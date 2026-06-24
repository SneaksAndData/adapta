from dataclasses import dataclass
import pytest
from adapta.dataclass_validation import Field


@dataclass
class TestInput:
    field_instance: Field


@dataclass
class TestOutput:
    expected_required: bool
    expected_settings: list[str]


@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        pytest.param(
            TestInput(field_instance=Field(display_name="v", description="d", dtype=str)),
            TestOutput(expected_required=True, expected_settings=[]),
            id="Default initialization sets required to True",
        ),
        pytest.param(
            TestInput(field_instance=Field(display_name="v", description="d", dtype=str, required=True)),
            TestOutput(expected_required=True, expected_settings=[]),
            id="Explicitly required field",
        ),
        pytest.param(
            TestInput(field_instance=Field(display_name="v", description="d", dtype=str, required=False)),
            TestOutput(expected_required=False, expected_settings=[]),
            id="Explicitly optional field",
        ),
        pytest.param(
            TestInput(field_instance=Field(display_name="v", description="d", dtype=str, required_by_settings=["s1"])),
            TestOutput(expected_required=False, expected_settings=["s1"]),
            id="Setting based requirement defaults required to False",
        ),
        pytest.param(
            TestInput(
                field_instance=Field(
                    display_name="v", description="d", dtype=str, required=True, required_by_settings=["s1"]
                )
            ),
            TestOutput(expected_required=False, expected_settings=["s1"]),
            id="Setting based requirement with explicit True",
        ),
        pytest.param(
            TestInput(
                field_instance=Field(
                    display_name="v", description="d", dtype=str, required=False, required_by_settings=["s1"]
                )
            ),
            TestOutput(expected_required=False, expected_settings=["s1"]),
            id="Setting based requirement with explicit False",
        ),
    ],
)
def test__field_initialization__unit_test(inputs: TestInput, expected: TestOutput):
    """
    Unit test for Field initialization logic:
    Verifies the internal state of the Field object after instantiation.
    """
    # Act
    field = inputs.field_instance

    # Assert
    assert field.required == expected.expected_required
    assert field.required_by_settings == expected.expected_settings
