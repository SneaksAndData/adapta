from dataclasses import dataclass
from unittest.mock import MagicMock
import polars as pl
from polars.testing import assert_frame_equal
import pytest

from adapta.dataclass_validation import AbstractDataClass, Field
from adapta.dataclass_validation.validation.validation_polars import PolarsValidationClass


@dataclass
class TestInput:
    target_field: Field
    dataframe: pl.DataFrame
    coerce_all: bool = True


@dataclass
class TestOutput:
    expected_dtype: pl.DataType
    expected_values: list


@pytest.mark.parametrize(
    ("inputs", "expected"),
    [
        # --- Integers (Target: Int64, Float64, or String) ---
        # Int8
        pytest.param(
            TestInput(
                target_field=Field(display_name="v", description="d", dtype=int),
                dataframe=pl.DataFrame({"v": [1]}, schema={"v": pl.Int8}),
            ),
            TestOutput(expected_dtype=pl.Int64, expected_values=[1]),
            id="Int8 to Int64",
        ),
        pytest.param(
            TestInput(
                target_field=Field(display_name="v", description="d", dtype=float),
                dataframe=pl.DataFrame({"v": [1]}, schema={"v": pl.Int8}),
            ),
            TestOutput(expected_dtype=pl.Float64, expected_values=[1.0]),
            id="Int8 to Float64",
        ),
        pytest.param(
            TestInput(
                target_field=Field(display_name="v", description="d", dtype=str),
                dataframe=pl.DataFrame({"v": [1]}, schema={"v": pl.Int8}),
            ),
            TestOutput(expected_dtype=pl.String, expected_values=["1"]),
            id="Int8 to String",
        ),
        # Int16
        pytest.param(
            TestInput(
                target_field=Field(display_name="v", description="d", dtype=int),
                dataframe=pl.DataFrame({"v": [1]}, schema={"v": pl.Int16}),
            ),
            TestOutput(expected_dtype=pl.Int64, expected_values=[1]),
            id="Int16 to Int64",
        ),
        pytest.param(
            TestInput(
                target_field=Field(display_name="v", description="d", dtype=float),
                dataframe=pl.DataFrame({"v": [1]}, schema={"v": pl.Int16}),
            ),
            TestOutput(expected_dtype=pl.Float64, expected_values=[1.0]),
            id="Int16 to Float64",
        ),
        pytest.param(
            TestInput(
                target_field=Field(display_name="v", description="d", dtype=str),
                dataframe=pl.DataFrame({"v": [1]}, schema={"v": pl.Int16}),
            ),
            TestOutput(expected_dtype=pl.String, expected_values=["1"]),
            id="Int16 to String",
        ),
        # Int32
        pytest.param(
            TestInput(
                target_field=Field(display_name="v", description="d", dtype=int),
                dataframe=pl.DataFrame({"v": [1]}, schema={"v": pl.Int32}),
            ),
            TestOutput(expected_dtype=pl.Int64, expected_values=[1]),
            id="Int32 to Int64",
        ),
        pytest.param(
            TestInput(
                target_field=Field(display_name="v", description="d", dtype=float),
                dataframe=pl.DataFrame({"v": [1]}, schema={"v": pl.Int32}),
            ),
            TestOutput(expected_dtype=pl.Float64, expected_values=[1.0]),
            id="Int32 to Float64",
        ),
        pytest.param(
            TestInput(
                target_field=Field(display_name="v", description="d", dtype=str),
                dataframe=pl.DataFrame({"v": [1]}, schema={"v": pl.Int32}),
            ),
            TestOutput(expected_dtype=pl.String, expected_values=["1"]),
            id="Int32 to String",
        ),
        # Int64
        pytest.param(
            TestInput(
                target_field=Field(display_name="v", description="d", dtype=float),
                dataframe=pl.DataFrame({"v": [1]}, schema={"v": pl.Int64}),
            ),
            TestOutput(expected_dtype=pl.Float64, expected_values=[1.0]),
            id="Int64 to Float64",
        ),
        pytest.param(
            TestInput(
                target_field=Field(display_name="v", description="d", dtype=str),
                dataframe=pl.DataFrame({"v": [1]}, schema={"v": pl.Int64}),
            ),
            TestOutput(expected_dtype=pl.String, expected_values=["1"]),
            id="Int64 to String",
        ),
        # --- Floats (Target: Float64, String, or Int64) ---
        # Float32
        pytest.param(
            TestInput(
                target_field=Field(display_name="v", description="d", dtype=float),
                dataframe=pl.DataFrame({"v": [1.5]}, schema={"v": pl.Float32}),
            ),
            TestOutput(expected_dtype=pl.Float64, expected_values=[1.5]),
            id="Float32 to Float64",
        ),
        pytest.param(
            TestInput(
                target_field=Field(display_name="v", description="d", dtype=str),
                dataframe=pl.DataFrame({"v": [1.5]}, schema={"v": pl.Float32}),
            ),
            TestOutput(expected_dtype=pl.String, expected_values=["1.5"]),
            id="Float32 to String",
        ),
        # Float64
        pytest.param(
            TestInput(
                target_field=Field(display_name="v", description="d", dtype=str),
                dataframe=pl.DataFrame({"v": [1.5]}, schema={"v": pl.Float64}),
            ),
            TestOutput(expected_dtype=pl.String, expected_values=["1.5"]),
            id="Float64 to String",
        ),
        # --- Booleans (Target: Int64 or String) ---
        pytest.param(
            TestInput(
                target_field=Field(display_name="v", description="d", dtype=int),
                dataframe=pl.DataFrame({"v": [True]}, schema={"v": pl.Boolean}),
            ),
            TestOutput(expected_dtype=pl.Int64, expected_values=[1]),
            id="Boolean to Int64",
        ),
        pytest.param(
            TestInput(
                target_field=Field(display_name="v", description="d", dtype=str),
                dataframe=pl.DataFrame({"v": [True]}, schema={"v": pl.Boolean}),
            ),
            TestOutput(expected_dtype=pl.String, expected_values=["true"]),
            id="Boolean to String",
        ),
    ],
)
def test__coerce_and_select_columns__casting_rules__unit_test(inputs: TestInput, expected: TestOutput):
    """
    Unit test for granular casting rules:
    Validates that AbstractDataClass correctly handles safe upcasts and
    string conversions defined in the _allowed_casts property.
    """

    class DynamicSchema(AbstractDataClass):
        v = inputs.target_field

    schema_instance = DynamicSchema()
    result = schema_instance.coerce_and_select_columns(data=inputs.dataframe, coerce_all=inputs.coerce_all)

    expected_dataframe = pl.DataFrame(data={"v": expected.expected_values}, schema={"v": expected.expected_dtype})
    assert_frame_equal(result, expected_dataframe)


def test__allowed_casts__convention_coverage():
    """
    Convention test ensuring every allowed cast mapping is covered by the unit test.
    """
    validator = PolarsValidationClass(data=MagicMock(), schema=MagicMock(), settings=MagicMock())

    required_pairs = set()
    for source, targets in validator._allowed_casts.items():
        for target in targets:
            # Normalize to the base class if it's an instance (like Datetime)
            s_base = source if isinstance(source, type) else source.__class__
            t_base = target if isinstance(target, type) else target.__class__
            required_pairs.add((s_base, t_base))

    marker = next(
        m for m in test__coerce_and_select_columns__casting_rules__unit_test.pytestmark if m.name == "parametrize"
    )
    test_params = marker.args[1]

    covered_pairs = set()
    for param in test_params:
        test_input = param.values[0]

        source_dtype = list(test_input.dataframe.schema.values())[0]
        # Normalize source
        s_base = source_dtype if isinstance(source_dtype, type) else source_dtype.__class__

        target_py_type = test_input.target_field.dtype
        target_dtype = validator._dtype_mapping.get(target_py_type, target_py_type)
        # Normalize target
        t_base = target_dtype if isinstance(target_dtype, type) else target_dtype.__class__

        covered_pairs.add((s_base, t_base))

    missing = required_pairs - covered_pairs
    assert not missing, f"Missing unit test coverage for _allowed_casts combinations: {missing}"
