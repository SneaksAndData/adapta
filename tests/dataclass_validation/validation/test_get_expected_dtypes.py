import datetime
import pytest
import polars as pl

from adapta.dataclass_validation import AbstractDataClass, Field
from adapta.dataclass_validation.validation.validation_polars import PolarsValidationClass


@pytest.mark.parametrize(
    "dtype, expected_polars_dtype",
    [
        (str, pl.String),
        (int, pl.Int64),
        (float, pl.Float64),
        (bool, pl.Boolean),
        (list[str], pl.List(pl.String)),
        (list[list[float]], pl.List(pl.List(pl.Float64))),
        (datetime.date, pl.Date),
        (datetime.datetime, pl.Datetime),
    ],
)
def test__polars_get_expected_types__expected(dtype: type, expected_polars_dtype: pl.DataType):
    """
    Test that the get_expected_types method returns the expected Polars dtypes for the given Python types.
    """

    class DummyDataClass(AbstractDataClass):
        column_1 = Field(
            display_name="Column 1",
            description="Description for column 1.",
            dtype=str,
            primary_key=True,
            required=True,
        )

    result = PolarsValidationClass(
        data=pl.DataFrame(),
        schema=DummyDataClass(),  # Schema is not used in this test
        settings=[],
    )._get_expected_dtypes(dtype)

    assert result == expected_polars_dtype


@pytest.mark.parametrize(
    "dtype",
    [
        dict,
    ],
)
def test__polars_get_expected_types__expected_errors(
    dtype: type,
):
    """
    Test that the get_expected_types method raised errors when an unsupported type is provided for the polars class.
    """

    class DummyDataClass(AbstractDataClass):
        column_1 = Field(
            display_name="Column 1",
            description="Description for column 1.",
            dtype=str,
            primary_key=True,
            required=True,
        )

    with pytest.raises(TypeError):
        PolarsValidationClass(
            data=pl.DataFrame(),
            schema=DummyDataClass(),  # Schema is not used in this test
            settings=[],
        )._get_expected_dtypes(dtype)
