"""
Validation class for Polars DataFrames.
"""
import datetime
from typing import Any

import polars as pl

from adapta.dataclass_validation.validation.validation_abstract import AbstractValidationClass


class PolarsValidationClass(AbstractValidationClass):
    """
    Polars Validation Class for validating data against a schema defined as an AbstractDataClass.
    """

    @property
    def _dtype_mapping(self):
        return {
            str: pl.String,
            int: pl.Int64,
            float: pl.Float64,
            bool: pl.Boolean,
            datetime.date: pl.Date,
            datetime.datetime: pl.Datetime,
            dict: pl.Struct,
        }

    @property
    def _dtype_recursive_dtypes(self):
        return {list: pl.List, dict: pl.Struct}

    @property
    def _allowed_casts(self):
        return {
            pl.Int8: [pl.Int64, pl.Float64, pl.String],
            pl.Int16: [pl.Int64, pl.Float64, pl.String],
            pl.Int32: [pl.Int64, pl.Float64, pl.String],
            pl.Int64: [pl.Float64, pl.String],
            pl.Float32: [pl.Float64, pl.String],
            pl.Float64: [pl.String],
            pl.Boolean: [pl.Int64, pl.String],
        }

    def _validate_primary_keys(self, **kwargs) -> None:
        primary_keys = self._schema.get_primary_keys()
        if primary_keys and len(self._data) != len(self._data.select(primary_keys).unique()):
            self._failed_validations += [
                "Duplicated primary key(s) found. Please ensure primary key(s) are unique. This is the provided "
                f"primary key(s): {primary_keys}"
            ]

    def _get_column_dtype(self, column_name: str) -> Any:
        return self._data[column_name].dtype

    def _get_dataframe_columns(self) -> list[str]:
        return self._data.columns

    def _add_column(self, column_name: str, dtype: type) -> None:
        self._data = self._data.with_columns(pl.lit(None).cast(self._get_expected_dtypes(dtype)).alias(column_name))

    def _cast_column(self, column_name: str, dtype: pl.DataType) -> None:
        self._data = self._data.with_columns(pl.col(column_name).cast(dtype).alias(column_name))

    def _are_values_ge(self, column_name: str, ge_value: float, tolerance: float) -> float | None:
        column_dtype = self._data[column_name].dtype
        within_tolerance = (pl.col(column_name) >= ge_value - tolerance) & (pl.col(column_name) < ge_value)
        self._data = self._data.with_columns(
            pl.when(within_tolerance)
            .then(pl.lit(ge_value).cast(column_dtype))
            .otherwise(pl.col(column_name))
            .alias(column_name)
        )
        failed = self._data.filter(pl.col(column_name) < ge_value)
        if failed.is_empty():
            return None
        return failed[column_name].min()

    def _are_values_le(self, column_name: str, le_value: float, tolerance: float) -> float | None:
        column_dtype = self._data[column_name].dtype
        within_tolerance = (pl.col(column_name) <= le_value + tolerance) & (pl.col(column_name) > le_value)
        self._data = self._data.with_columns(
            pl.when(within_tolerance)
            .then(pl.lit(le_value).cast(column_dtype))
            .otherwise(pl.col(column_name))
            .alias(column_name)
        )
        failed = self._data.filter(pl.col(column_name) > le_value)
        if failed.is_empty():
            return None
        return failed[column_name].max()

    def _are_values_not_missing(self, column_name: str) -> bool:
        return self._data.filter(pl.col(column_name).is_null()).is_empty()

    def _get_invalid_enum_members(
        self, column_name: str, enum_members: list, dtype: type, allow_missing_values: bool
    ) -> list:
        """
        Find column values that are not valid enum members.

        :param column_name: The name of the column to check.
        :param enum_members: The list of enum member values for the column.
        :param dtype: The Python type of the field, used to filter enum members to matching types.
        :param allow_missing_values: Whether null values are allowed for the column. If True, nulls are excluded
            from the check. If False, nulls are treated as invalid enum members.
        :return: A list of invalid values found in the column, empty if all values are valid.
        """
        dtype_enum_members = [value for value in enum_members if isinstance(value, dtype)]
        invalid_condition = ~pl.col(column_name).is_in(dtype_enum_members)
        if allow_missing_values:
            invalid_condition = invalid_condition & pl.col(column_name).is_not_null()
        invalid = self._data.filter(invalid_condition)
        if invalid.is_empty():
            return []
        return invalid[column_name].unique().to_list()

    def _are_list_values_ge(self, column_name: str, ge_value: float, tolerance: float) -> float | None:
        """
        Check if all elements within a list column are greater than or equal to the specified value,
        fixing values within tolerance to the bound.

        :param column_name: The name of the list column to check.
        :param ge_value: The minimum allowed value.
        :param tolerance: The tolerance for values near the bound.
        :return: None if all values pass, otherwise the minimum failing value.
        """
        inner_dtype = self._data[column_name].dtype.inner
        within_tolerance = (pl.element() >= ge_value - tolerance) & (pl.element() < ge_value)
        self._data = self._data.with_columns(
            pl.col(column_name)
            .list.eval(pl.when(within_tolerance).then(pl.lit(ge_value).cast(inner_dtype)).otherwise(pl.element()))
            .alias(column_name)
        )
        min_per_row = self._data.select(pl.col(column_name).list.min().alias(column_name))
        failed = min_per_row.filter(pl.col(column_name) < ge_value)
        if failed.is_empty():
            return None
        return failed[column_name].min()

    def _are_list_values_le(self, column_name: str, le_value: float, tolerance: float) -> float | None:
        """
        Check if all elements within a list column are less than or equal to the specified value,
        fixing values within tolerance to the bound.

        :param column_name: The name of the list column to check.
        :param le_value: The maximum allowed value.
        :param tolerance: The tolerance for values near the bound.
        :return: None if all values pass, otherwise the maximum failing value.
        """
        inner_dtype = self._data[column_name].dtype.inner
        within_tolerance = (pl.element() <= le_value + tolerance) & (pl.element() > le_value)
        self._data = self._data.with_columns(
            pl.col(column_name)
            .list.eval(pl.when(within_tolerance).then(pl.lit(le_value).cast(inner_dtype)).otherwise(pl.element()))
            .alias(column_name)
        )
        max_per_row = self._data.select(pl.col(column_name).list.max().alias(column_name))
        failed = max_per_row.filter(pl.col(column_name) > le_value)
        if failed.is_empty():
            return None
        return failed[column_name].max()

    def get_data_for_columns(self) -> pl.DataFrame:
        """
        Get the data and select all columns.
        """
        return self.get_data().select(self._schema.get_columns())

    def get_data_for_required_columns(self) -> pl.DataFrame:
        """
        Get the data and select required columns.
        """
        return self.get_data().select(self._schema.get_required_columns())
