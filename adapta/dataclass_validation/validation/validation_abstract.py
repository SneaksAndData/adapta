"""
Abstract Validation Class
"""
from abc import abstractmethod
from typing import Any, get_origin, get_args

from adapta.dataclass_validation.dataclass.dataclass_core import CoreDataClass, Field
from adapta.dataclass_validation.validation.validation_utils import ValidationResponse


class AbstractValidationClass:
    """
    Abstract Validation Class
    """

    def __init__(self, data: Any, schema: CoreDataClass, settings: list[str], add_non_required_fields: bool = False):
        self._data = data
        self._schema = schema
        self._settings = settings
        self._required_fields = schema.get_required_fields(settings=settings)
        self._allowed_adding_missing_fields = schema.get_allowed_fields_to_add()
        self._non_required_fields_should_be_added = add_non_required_fields
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

    def _get_expected_dtypes(self, dtype: type) -> Any:
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
                    self._failed_validation_columns += [field_name]

    @abstractmethod
    def _get_dataframe_columns(self) -> list[str]:
        """
        Abstract method to get the columns of the dataframe.
        """

    @abstractmethod
    def _get_column_dtype(self, column_name: str) -> Any:
        """
        Abstract method to get the data type of a specific column.
        """

    @abstractmethod
    def _add_column(self, column_name: str, dtype: type) -> None:
        """
        Abstract method for adding a column to the data.
        """

    @abstractmethod
    def _cast_column(self, column_name: str, dtype: Any) -> None:
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

    def _add_non_required_fields(self) -> None:
        """Adds non-required missing fields to the dataframe."""
        non_required_missing_fields = (
            set(self._schema.get_fields()) - set(self._required_fields) - set(self._get_dataframe_columns())
        )
        for field_name in non_required_missing_fields:
            field = self._schema.get_fields()[field_name]
            self._add_column(column_name=field_name, dtype=field.dtype)

    @abstractmethod
    def _are_values_ge(self, column_name: str, ge_value: float, tolerance: float) -> float | None:
        """
        Abstract method to check if a column has values greater than or equal to a specified value,
        fixing values within tolerance to the bound. Returns None if all values satisfy the condition, otherwise
        returns the minimum value that fails the condition.
        """

    @abstractmethod
    def _are_values_le(self, column_name: str, le_value: float, tolerance: float) -> float | None:
        """
        Abstract method to check if a value is less than or equal to a specified value,
        fixing values within tolerance to the bound. Returns None if all values satisfy the condition, otherwise
        returns the maximum value that fails the condition.
        """

    @abstractmethod
    def _are_values_not_missing(self, column_name: str) -> bool:
        """
        Abstract method to check if a value is not missing.
        """

    @staticmethod
    def _is_list_numeric_field(field: Field) -> bool:
        """
        Check if a field's dtype is a list of numeric types (list[int] or list[float]).

        :param field: The field to check.
        :return: True if the field dtype is list[int] or list[float].
        """
        return get_origin(field.dtype) is list and get_args(field.dtype)[0] in (int, float)

    @abstractmethod
    def _are_list_values_ge(self, column_name: str, ge_value: float, tolerance: float) -> float | None:
        """
        Abstract method to check if all elements within a list column satisfy the greater than or equal
        to constraint, fixing values within tolerance to the bound. Returns None if all values satisfy
        the condition, otherwise returns the minimum value that fails the condition.

        :param column_name: The name of the list column to check.
        :param ge_value: The minimum allowed value.
        :param tolerance: The tolerance for values near the bound.
        :return: None if all values pass, otherwise the minimum failing value.
        """

    @abstractmethod
    def _are_list_values_le(self, column_name: str, le_value: float, tolerance: float) -> float | None:
        """
        Abstract method to check if all elements within a list column satisfy the less than or equal
        to constraint, fixing values within tolerance to the bound. Returns None if all values satisfy
        the condition, otherwise returns the maximum value that fails the condition.

        :param column_name: The name of the list column to check.
        :param le_value: The maximum allowed value.
        :param tolerance: The tolerance for values near the bound.
        :return: None if all values pass, otherwise the maximum failing value.
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
            if self._should_validate_field(field_name=field_name):
                if self._is_list_numeric_field(field=field):
                    result = self._are_list_values_ge(
                        column_name=field_name,
                        ge_value=field.checks.ge_value,
                        tolerance=field.checks.ge_value_tolerance,
                    )
                else:
                    result = self._are_values_ge(
                        column_name=field_name,
                        ge_value=field.checks.ge_value,
                        tolerance=field.checks.ge_value_tolerance,
                    )
                if result is not None:
                    self._failed_validations += [
                        f"Column '{field_name}' does not satisfy the greater than or equal to constraint. "
                        f"It should be greater than {field.checks.ge_value}, but found minimum value {result}."
                    ]

    def _validate_le_value(self) -> None:
        for field_name, field in self._schema.get_le_value_fields().items():
            if self._should_validate_field(field_name=field_name):
                if self._is_list_numeric_field(field=field):
                    result = self._are_list_values_le(
                        column_name=field_name,
                        le_value=field.checks.le_value,
                        tolerance=field.checks.le_value_tolerance,
                    )
                else:
                    result = self._are_values_le(
                        column_name=field_name,
                        le_value=field.checks.le_value,
                        tolerance=field.checks.le_value_tolerance,
                    )
                if result is not None:
                    self._failed_validations += [
                        f"Column '{field_name}' does not satisfy the less than or equal to constraint. "
                        f"It should be less than {field.checks.le_value}, but found maximum value {result}."
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

    @abstractmethod
    def _get_invalid_enum_members(
        self, column_name: str, enum_members: list[Any], dtype: type, allow_missing_values: bool
    ) -> list:
        """
        Abstract method to find column values that are not valid enum members.

        :param column_name: The name of the column to check.
        :param enum_members: The list of enum member values for the column.
        :param dtype: The Python type of the field, used to filter enum members to matching types.
        :param allow_missing_values: Whether null values are allowed for the column. If True, nulls are excluded
            from the check. If False, nulls are treated as invalid enum members.
        :return: A list of invalid values found in the column.
        """

    def _validate_enum_members(self) -> None:
        """
        Validate that column values belong to the set of allowed enum member values.
        """
        for field_name, field in self._schema.get_enum_fields().items():
            if self._should_validate_field(field_name=field_name):
                enum_member_values = [member.value for member in field.enum]
                invalid_values = self._get_invalid_enum_members(
                    column_name=field_name,
                    enum_members=enum_member_values,
                    dtype=field.dtype,
                    allow_missing_values=field.allow_missing_values,
                )
                if invalid_values:
                    self._failed_validations += [
                        f"Column '{field_name}' contains values that are not members of "
                        f"{field.enum.__name__}. Invalid values found: {invalid_values}"
                    ]

    def _set_failed_validations(self) -> None:
        self._add_missing_fields()
        self._validate_missing_fields()
        if self._non_required_fields_should_be_added:
            self._add_non_required_fields()

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
        self._validate_enum_members()

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

    def get_data(self) -> Any:
        """
        Get the data for all columns
        """
        return self._data

    @abstractmethod
    def get_data_for_columns(self) -> Any:
        """
        Get the data and select all columns.
        """

    @abstractmethod
    def get_data_for_required_columns(self) -> Any:
        """
        Get the data and select required columns.
        """
