from dataclasses import dataclass
import polars as pl

from adapta.dataclass_validation.dataclass.dataclass_core import CoreDataClass


@dataclass
class ValidationResponse:
    """
    Class to hold validation response.
    """

    schema_name: str
    data: pl.DataFrame
    schema: CoreDataClass
    failed_validations: list[str]


def raise_failed_validations(failed_validations: list[ValidationResponse]) -> None:
    """
    Raises the failed validations in a readable format.
    """
    if len(failed_validations) > 0 and any(len(validation.failed_validations) > 0 for validation in failed_validations):
        raise ValueError(
            "\n\n".join(
                [
                    f"Dataframe validation for schema {validation.schema_name} failed with "
                    f"{len(validation.failed_validations)} error(s):\n"
                    + "\n".join(
                        [
                            f"{i}: {error_message}"
                            for i, error_message in enumerate(validation.failed_validations, start=1)
                        ]
                    )
                    for validation in failed_validations
                    if len(validation.failed_validations) > 0
                ]
            )
        )
