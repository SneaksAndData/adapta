import datetime

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
        }

    @property
    def _dtype_recursive_dtypes(self):
        return {list: pl.List}

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
            self._failed_validations = [
                "Duplicated primary key(s) found. Please ensure primary key(s) are unique. This is the provided "
                f"primary key(s): {primary_keys}"
            ]

    def _get_column_dtype(self, column_name: str) -> any:
        return self._data[column_name].dtype

    def _get_dataframe_columns(self) -> list[str]:
        return self._data.columns

    def _add_column(self, column_name: str, dtype: type) -> None:
        self._data = self._data.with_columns(pl.lit(None).cast(self._get_expected_dtypes(dtype)).alias(column_name))

    def _cast_column(self, column_name: str, dtype: pl.DataType) -> None:
        self._data = self._data.with_columns(pl.col(column_name).cast(dtype).alias(column_name))

    def _are_values_ge(self, column_name: str, ge_value: float) -> bool:
        return self._data.filter(pl.col(column_name) < ge_value).is_empty()

    def _are_values_le(self, column_name: str, le_value: float) -> bool:
        return self._data.filter(pl.col(column_name) > le_value).is_empty()

    def _are_values_not_missing(self, column_name: str) -> bool:
        return self._data.filter(pl.col(column_name).is_null()).is_empty()

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
