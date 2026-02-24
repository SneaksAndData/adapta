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
