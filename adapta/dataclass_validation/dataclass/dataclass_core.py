"""
Core data class implementation for the adapta library
"""
from typing import final


@final
class Checks:
    """
    Check class to perform additional checks on fields.
    1. ge_value: Greater than or equal to constraint for the field (default is None).
    2. le_value: Less than or equal to constraint for the field (default is None).
    """

    def __init__(
        self,
        ge_value: float = None,
        le_value: float = None,
    ):
        self.ge_value = ge_value
        self.le_value = le_value


@final
class AstraProperties:
    """
    AstraProperties class to represent properties of the Astra database.
    """

    def __init__(
        self,
        partition_key: bool = None,
        custom_index: bool = None,
        vector_enabled: bool = None,
    ):
        self.partition_key = partition_key
        self.custom_index = custom_index
        self.vector_enabled = vector_enabled


@final
class Field:
    """
    Field class to represent a field in the OR data model.
    1. display_name: The name of the field.
    2. description: A brief description of the field.
    3. dtype: The data type of the field (e.g., str, int, float).
    4. primary_key: Whether the field is a primary key (default is False).
    5. required: Whether the field is required (default is True) - overwritten to false if
    required_by_settings is provided.
    6. required_by_settings: A list of settings that require this field (default is an empty list).
    7. coerce: Whether the field is allowed to be coerced (default is False).
    8. add_field_if_missing: Whether to add the field if it is missing (default is False). If true, we add a column
        with None values with the correct dtype.
    9. checks: Check object to perform additional checks on the field (default is None).
    10. allow_missing_values: Whether missing values (e.g. None) are allowed (default is False).
    """

    # pylint: disable=too-many-arguments, too-many-instance-attributes
    def __init__(
        self,
        display_name: str,
        description: str,
        dtype: type,
        primary_key: bool = False,
        required: bool = True,
        required_by_settings: list[str] = None,
        coerce: bool = False,
        add_field_if_missing: bool = False,
        checks: Checks = None,
        allow_missing_values: bool = False,
        astra_properties: AstraProperties = None,
    ):
        self.display_name = display_name
        self.description = description
        self.dtype = dtype
        self.primary_key = primary_key
        self.required = required
        self.required_by_settings = required_by_settings if required_by_settings is not None else []
        self.coerce = coerce
        self.add_field_if_missing = add_field_if_missing
        self.checks = checks
        self.allow_missing_values = allow_missing_values
        self.astra_properties = astra_properties

        if self.checks is not None and self.dtype not in [int, float]:
            if self.checks.le_value is not None or self.checks.ge_value is not None:
                raise ValueError("Field checks can only be applied to numeric fields (int or float).")

        if self.primary_key and self.allow_missing_values:
            raise ValueError("Primary keys cannot allow missing values.")

        if self.primary_key and not self.required:
            raise ValueError("Primary keys must be required.")

        if self.add_field_if_missing and not self.allow_missing_values:
            raise ValueError("If add_field_if_missing is True, allow_missing_values must be True.")

        # if required_by_settings is provided, we set required to False, since the field is not always required,
        # but only required based on the settings provided.
        if self.required and self.required_by_settings:
            self.required = False


class CoreDataClass:
    """
    Core Data Class with common functionality for all data classes.
    """

    def __init__(self, coerce_all: bool = False) -> None:
        """
        Initialize the abstract data class, setting up fields and primary keys.
        """
        self._set_fields()
        self._set_primary_keys()
        self._set_python_schema_entity_field_names()
        self.set_coerce_fields(coerce_all=coerce_all)
        self._set_ge_value_fields()
        self._set_le_value_fields()
        self._set_not_allowed_missing_value_fields()
        self._set_astra_properties_keys()

    def _set_fields(self) -> None:
        """
        Set the fields of the data class by extracting them from the class dictionary.
        """
        # Set fields directly defined in current class
        self._fields = {name: value for name, value in self.__class__.__dict__.items() if isinstance(value, Field)}

        # Set fields from parent classes
        for base in reversed(self.__class__.__mro__):
            if base.__name__ == self.__class__.__name__:
                continue
            for name, value in base.__dict__.items():
                if isinstance(value, Field):
                    if name in self._fields:
                        raise ValueError(
                            f"Field name '{name}' is defined multiple times in the class hierarchy of "
                            f"{self.__class__.__name__}."
                        )

                    self._fields[name] = value

    def _set_python_schema_entity_field_names(self) -> None:
        """
        Set the field names to match the Python schema entity field names.
        """
        for field_name in self._fields.keys():
            setattr(self, field_name, field_name)

    def _set_primary_keys(self) -> None:
        """
        Set the primary keys of the data class by checking
        """
        self._primary_keys = [field_name for field_name, field in self.get_fields().items() if field.primary_key]

        required_fields = {field_name for field_name, field in self._fields.items() if field.required}
        if len(set(self._primary_keys) - set(required_fields)) > 0:
            raise ValueError(f"All primary keys for {self.__class__.__name__} must be always required.")

    def _set_astra_properties_keys(self):
        """
        Set the Astra properties keys of the data class by checking which fields have Astra properties defined.
        """

        self._astra_properties_field = {
            field_name: field.astra_properties
            for field_name, field in self.get_fields().items()
            if field.astra_properties is not None
        }

        if len(self.get_astra_properties_field().keys()) > 0 and len(self.get_primary_keys()) == 0:
            raise ValueError(
                f"Data class {self.__class__.__name__} must have at least one primary key defined to use Astra "
                "properties."
            )

        self._astra_partition_keys = [
            field_name
            for field_name, astra_properties in self.get_astra_properties_field().items()
            if astra_properties.partition_key
        ]
        self._astra_custom_index_keys = [
            field_name
            for field_name, astra_properties in self.get_astra_properties_field().items()
            if astra_properties.custom_index
        ]
        self._astra_vector_enabled_keys = [
            field_name
            for field_name, astra_properties in self.get_astra_properties_field().items()
            if astra_properties.vector_enabled
        ]

    def set_coerce_fields(self, coerce_all: bool) -> None:
        """
        Set the fields that are allowed to be coerced.
        """
        if coerce_all:
            self._coerce_fields = self.get_fields()
        else:
            self._coerce_fields = {field_name: field for field_name, field in self.get_fields().items() if field.coerce}

    def _set_ge_value_fields(self) -> None:
        """
        Set the fields that have a greater than or equal to constraint.
        """
        self._ge_value_fields = {
            field_name: field
            for field_name, field in self.get_fields().items()
            if field.checks is not None and field.checks.ge_value is not None
        }

    def _set_le_value_fields(self) -> None:
        """
        Set the fields that have a less than or equal to constraint.
        """
        self._le_value_fields = {
            field_name: field
            for field_name, field in self.get_fields().items()
            if field.checks is not None and field.checks.le_value is not None
        }

    def _set_not_allowed_missing_value_fields(self) -> None:
        """
        Set the fields that do not allow missing values.
        """
        self._not_allowed_missing_value_fields = {
            field_name: field for field_name, field in self.get_fields().items() if not field.allow_missing_values
        }

    def get_fields(self) -> dict[str, Field]:
        """
        Get the fields of the data class.
        """

        return self._fields

    def get_primary_keys(self) -> list[str]:
        """
        Get the primary keys of the data class.
        """

        return self._primary_keys

    def get_astra_properties_field(self) -> dict[str, AstraProperties]:
        """
        Get the Astra properties of the data class.
        """
        return self._astra_properties_field

    def get_astra_partition_keys(self) -> list[str]:
        """
        Get the partition keys of the data class.
        """
        return self._astra_partition_keys

    def get_astra_custom_index_keys(self) -> list[str]:
        """
        Get the custom index keys of the data class.
        """
        return self._astra_custom_index_keys

    def get_astra_vector_enabled_keys(self) -> list[str]:
        """
        Get the vector enabled keys of the data class.
        """
        return self._astra_vector_enabled_keys

    def get_required_fields(self, settings: list[str]) -> dict[str, Field]:
        """
        Get the required fields of the data class based on the provided settings.
        """

        return {
            field_name: field
            for field_name, field in self._fields.items()
            if field.required or any(setting in settings for setting in field.required_by_settings)
        }

    def get_allowed_fields_to_add(self) -> dict[str, Field]:
        """
        Get the allowed missing fields to add based on the provided settings.
        """

        return {field_name: field for field_name, field in self._fields.items() if field.add_field_if_missing}

    def get_coerce_fields(self) -> dict[str, Field]:
        """
        Get the fields that are allowed to be coerced.
        """
        return self._coerce_fields

    def get_ge_value_fields(self) -> dict[str, Field]:
        """
        Get the fields that have a greater than or equal to constraint.
        """
        return self._ge_value_fields

    def get_le_value_fields(self) -> dict[str, Field]:
        """
        Get the fields that have a less than or equal to constraint.
        """
        return self._le_value_fields

    def get_not_allowed_missing_value_fields(self) -> dict[str, Field]:
        """
        Get the fields that do not allow missing values.
        """
        return self._not_allowed_missing_value_fields

    def get_columns(self) -> list[str]:
        """
        Get all columns of the data class.
        """
        return list(self.get_fields().keys())

    def get_required_columns(self, settings: list[str] = None) -> list[str]:
        """
        Get the required columns of the data class based on the provided settings.
        """
        return list(self.get_required_fields(settings=settings if settings is not None else []).keys())

    def get_column_types(self) -> dict[str, type]:
        """
        Get all column types as a dictionary mapping field names to their data types.
        """
        fields = self.get_fields()
        return {field_name: field.dtype for field_name, field in fields.items()}

    def get_required_column_types(self, settings: list[str] = None) -> dict[str, type]:
        """
        Get required column types as a dictionary mapping field names to their data types
        based on the provided settings.
        """
        fields = self.get_required_fields(settings=settings if settings is not None else [])
        return {field_name: field.dtype for field_name, field in fields.items()}
