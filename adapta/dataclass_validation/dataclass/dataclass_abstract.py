"""
Abstract Data Class
"""
from copy import deepcopy

import polars as pl

from adapta.dataclass_validation.dataclass.dataclass_core import CoreDataClass
from adapta.dataclass_validation.validation.validation_utils import (
    raise_failed_validations,
    ValidationResponse,
)
from adapta.dataclass_validation.validation.validation_polars import PolarsValidationClass


class AbstractDataClass(CoreDataClass):
    """
    Abstract Data Class
    """

    def _validate_single_data(self, data: any, settings: list[str]) -> ValidationResponse:
        """
        Method for validating the data against the schema.
        """
        validation_response = None
        if isinstance(data, pl.DataFrame):
            validation_response = PolarsValidationClass(
                data=data,
                schema=self,
                settings=settings,
            ).validate()

        if validation_response is None:
            raise ValueError(f"Validation of dataframe with datatype {type(data)} is not supported!")

        return validation_response

    def validate_and_collect_data(self, data: any, settings: list[str] = None) -> ValidationResponse:
        """
        Method for validating the data against the schema.
        This method returns a ValidationResponse object containing the results of the validation.
        This method DOES NOT raise an exception if the validation fails, but collects them.
        """
        return self._validate_single_data(data=data, settings=settings if settings is not None else [])

    def validate_data(self, data: any, settings: list[str] = None) -> any:
        """
        Method for validating the data against the schema.
        This method returns the updated data if the validation is successful.
        This method RAISES an exception if the validation fails.
        """
        validation_response = self._validate_single_data(data=data, settings=settings if settings is not None else [])

        raise_failed_validations(failed_validations=[validation_response])

        return validation_response.data

    def create_empty_polars_dataframe(self) -> pl.DataFrame:
        """
        Create an empty Polars DataFrame based on the schema.
        """

        return pl.DataFrame(schema=self.get_column_types())

    def coerce_and_select_columns(self, data: any, coerce_all: bool = True) -> any:
        """
        Coerce the input data to match the schema and select only the columns defined in the schema.
        """
        internal_schema = deepcopy(self)
        internal_schema.set_coerce_fields(coerce_all=coerce_all)

        validation_class = None
        if isinstance(data, pl.DataFrame):
            validation_class = PolarsValidationClass(
                data=data,
                schema=internal_schema,
                settings=[],
            )

        if validation_class is None:
            raise ValueError(f"Coercion of dataframe with datatype {type(data)} is not supported!")

        validation_class.coerce_data_types(should_raise=True)

        return validation_class.get_data()
