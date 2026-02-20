"""
Abstract Validation Class
"""
from abc import abstractmethod
from typing import get_origin, get_args

from adapta.dataclass_validation.dataclass.dataclass_core import CoreDataClass
from adapta.dataclass_validation.validation.validation_utils import ValidationResponse


class AbstractValidationClass:
    """
    Abstract Validation Class
    """

    def __init__(self, data: any, schema: CoreDataClass, settings: list[str]):
        self._data = data
        self._schema = schema
        self._settings = settings
        self._required_fields = schema.get_required_fields(settings=settings)
        self._allowed_adding_missing_fields = schema.get_allowed_fields_to_add()
        self._failed_validations = []
        self._failed_validation_columns = []

    @property
    @abstractmethod
    def _dtype_mapping(self):
        pass

    @property
    @abstractmethod
    def _dtype_recursive_dtypes(self):
        pass

    @property
    @abstractmethod
    def _allowed_casts(self):
        pass

    def _validate_missing_fields(self) -> None:
        """
        Abstract method for validating missing fields.
        """
        missing_fields = set(self._required_fields) - set(self._get_dataframe_columns())
        if missing_fields:
            for missing_field in missing_fields:
                field = self._schema.get_fields()[missing_field]
                required_by_settings = [setting for setting in field.required_by_settings if setting in self._settings]
                if required_by_settings:
                    self._failed_validations += [
                        f"Missing required column: {missing_field} (required by settings: {required_by_settings})"
                    ]
                else:
                    self._failed_validations += [f"Missing required column: {missing_field}"]

    @abstractmethod
    def _validate_primary_keys(self, **kwargs) -> None:
        """
        Abstract method for validating primary keys.
        """

    def _get_expected_dtypes(self, dtype: type) -> any:
        """
        Method to get the expected data types for the fields. If the type is a list, we use recursion to get the
        expected type.
        """
        if dtype in self._dtype_mapping:
            return self._dtype_mapping[dtype]

        origin_dtype = get_origin(dtype)
        if origin_dtype in self._dtype_recursive_dtypes and origin_dtype == list:
            return self._dtype_recursive_dtypes[origin_dtype](self._get_expected_dtypes(dtype=get_args(dtype)[0]))

        raise TypeError(
            f"Unsupported data type: {dtype}. Supported types are: "
            f"{list(self._dtype_mapping.keys()) + list(self._dtype_recursive_dtypes.keys())}"
        )

    def _validate_data_types(self) -> None:
        """
        Abstract method for validating data types.
        """

        for field_name, field in self._required_fields.items():
            if field_name in self._get_dataframe_columns():
                expected_dtype = self._get_expected_dtypes(dtype=field.dtype)
                current_dtype = self._get_column_dtype(column_name=field_name)
                if current_dtype != expected_dtype:
                    self._failed_validations += [
                        f"Column '{field_name}' has incorrect type. Expected {expected_dtype}, got {current_dtype}"
                    ]

    @abstractmethod
    def _get_dataframe_columns(self) -> list[str]:
        """
        Abstract method to get the columns of the dataframe.
        """

    @abstractmethod
    def _get_column_dtype(self, column_name: str) -> any:
        """
        Abstract method to get the data type of a specific column.
        """

    @abstractmethod
    def _add_column(self, column_name: str, dtype: type) -> None:
        """
        Abstract method for adding a column to the data.
        """

    @abstractmethod
    def _cast_column(self, column_name: str, dtype: any) -> None:
        """
        Abstract method for casting a column to a specific type.
        """

    def coerce_data_types(self, should_raise: bool) -> None:
        """
        Method for coercing data types.
        """
        for field_name, field in self._schema.get_coerce_fields().items():
            expected_dtype = self._get_expected_dtypes(field.dtype)
            current_dtype = self._get_column_dtype(column_name=field_name)
            if all(
                [
                    current_dtype != expected_dtype,
                    field_name in self._get_dataframe_columns(),
                ]
            ):
                allowed_casts = self._allowed_casts.get(current_dtype, [])
                if expected_dtype not in allowed_casts:
                    if should_raise:
                        raise TypeError(
                            f"Cannot coerce column '{field_name}' from type {current_dtype} to type {expected_dtype}. "
                            f"Allowed casts from {current_dtype} are"
                            + (f": {allowed_casts}" if allowed_casts else " not defined.")
                        )
                    self._failed_validations += [
                        f"Cannot coerce column '{field_name}' from type {current_dtype} to type {expected_dtype}. "
                        f"Allowed casts from {current_dtype} are "
                        + (f": {allowed_casts}" if allowed_casts else " not defined.")
                    ]
                    self._failed_validation_columns += [field_name]
                    continue

                try:
                    self._cast_column(column_name=field_name, dtype=expected_dtype)
                # pylint: disable=broad-exception-caught
                except Exception as e:
                    if should_raise:
                        raise TypeError(
                            f"Failed to coerce column '{field_name}' from type {current_dtype} to type "
                            f"{expected_dtype}. Error: {str(e)}"
                        ) from e

                    self._failed_validations += [
                        f"Failed to coerce column '{field_name}' from type {current_dtype} from type {expected_dtype}. "
                        f"Error: {str(e)}"
                    ]
                    self._failed_validation_columns += [field_name]

    def _add_missing_fields(self) -> None:
        for field_name, field in self._allowed_adding_missing_fields.items():
            if field_name not in self._get_dataframe_columns():
                self._add_column(column_name=field_name, dtype=field.dtype)

    @abstractmethod
    def _are_values_ge(self, column_name: str, ge_value: float) -> bool:
        """
        Abstract method to check if a column has values greater than or equal to a specified value.
        """

    @abstractmethod
    def _are_values_le(self, column_name: str, le_value: float) -> bool:
        """
        Abstract method to check if a value is less than or equal to a specified value.
        """

    @abstractmethod
    def _are_values_not_missing(self, column_name: str) -> bool:
        """
        Abstract method to check if a value is not missing.
        """

    def _should_validate_field(self, field_name: str) -> bool:
        """
        Method to check if a field should be validated.
        1) A field should not be validated if it has already failed a previous validation.
        2) A field should not be validated if it's not present in the data
            (if it was required, it has failed before this stage)
        3) A field should not be validated if it's not required
        """

        return all(
            [
                field_name not in self._failed_validation_columns,
                field_name in self._get_dataframe_columns(),
                field_name in self._required_fields,
            ]
        )

    def _validate_ge_value(self) -> None:
        for field_name, field in self._schema.get_ge_value_fields().items():
            if self._should_validate_field(field_name=field_name) and not self._are_values_ge(
                column_name=field_name, ge_value=field.checks.ge_value
            ):
                self._failed_validations += [
                    f"Column '{field_name}' does not satisfy the greater than or equal to constraint. It should "
                    f"be greater than {field.checks.ge_value}."
                ]

    def _validate_le_value(self) -> None:
        for field_name, field in self._schema.get_le_value_fields().items():
            if self._should_validate_field(field_name=field_name) and not self._are_values_le(
                column_name=field_name, le_value=field.checks.le_value
            ):
                self._failed_validations += [
                    f"Column '{field_name}' does not satisfy the less than or equal to constraint. It should "
                    f"be less than {field.checks.le_value}."
                ]

    def _validate_value_not_missing(self) -> None:
        """
        Validate that values are not missing for fields that do not allow missing values.
        """
        for field_name in self._schema.get_not_allowed_missing_value_fields().keys():
            if self._should_validate_field(field_name=field_name) and not self._are_values_not_missing(
                column_name=field_name
            ):
                self._failed_validations += [
                    f"Column '{field_name}' does not allow missing values but contains missing values."
                ]

    def _set_failed_validations(self) -> None:
        self._add_missing_fields()

        self._validate_missing_fields()
        if len(self._failed_validations) > 0:
            return

        self._validate_primary_keys()
        if len(self._failed_validations) > 0:
            return

        self.coerce_data_types(should_raise=False)  # Do not raise errors during coercion, just collect them
        self._validate_data_types()
        self._validate_ge_value()
        self._validate_le_value()
        self._validate_value_not_missing()

    def validate(self) -> ValidationResponse:
        """
        Method for validating the data against the schema.
        """
        self._set_failed_validations()

        return ValidationResponse(
            schema_name=self._schema.__class__.__name__,
            data=self._data,
            schema=self._schema,
            failed_validations=self._failed_validations,
        )

    def get_data(self) -> any:
        """
        Get the data for all columns
        """
        return self._data

    @abstractmethod
    def get_data_for_columns(self) -> any:
        """
        Get the data and select all columns.
        """

    @abstractmethod
    def get_data_for_required_columns(self) -> any:
        """
        Get the data and select required columns.
        """
