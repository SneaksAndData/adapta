from adapta.dataclass_validation.dataclass.dataclass_abstract import AbstractDataClass
from adapta.dataclass_validation.validation.validation_utils import (
    ValidationResponse,
    raise_failed_validations,
)


class ValidationClass:
    """
    Validation Class for validating data against a schema defined as an AbstractDataClass.
    """

    def __init__(self):
        """
        Initialize the abstract data class, setting up fields and primary keys.
        """
        self._failed_validations: [ValidationResponse] = []

    def validate(self, validations: list[tuple[any, AbstractDataClass]], settings: list[str]) -> None:
        """
        Validate a list of dataframes against their corresponding schemas.
        """
        for data, schema in validations:
            self._failed_validations += [schema.validate_and_collect_data(data=data, settings=settings)]

        raise_failed_validations(failed_validations=self._failed_validations)
